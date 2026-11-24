"""洞察工坊 · 报告数据集绑定测试

覆盖题目要求的完整业务链路：

1. 新增报告数据集绑定，按指标保存关联方案和真实结果版本；
2. 报告计算按绑定取每项指标自己实际关联后的明细再聚合，
   收入与退款即使字段都叫 amount 也各用各的已核验明细；
3. 月报确认后重新进入报告，读取持久化绑定并核对实际关联行，
   不被最新一次保存的主表或其他指标覆盖；
4. 收入关联结果算 60、退款关联结果算 15；另一份合法收入关系算 70，
   任何一项都不能被排在最后的主表数据覆盖；
5. 已领用旧绑定后、确认月报前，收入关系更正与当期签收冲突：
   继续按旧绑定能报 60/15，采用已核验新绑定能报 70/15；
6. 同一绑定版本的两项结果一起确认，未经核验的更正不得替换可用绑定；
7. 并发确认同一次月报只写一次，旧报告仍能按实际历史行算回原值；
8. 存入后重新读取两个绑定关系，运行真实关联与报告计算并交错收入更正，
   核对正式报告明细、历史金额和另一个指标的真实读数。
"""
from __future__ import annotations

import copy
import threading

import pytest

from app.report_binding import (
    BINDING_CORRECTING,
    BINDING_DRAFT,
    BINDING_VERIFIED,
    BindingError,
    BindingRegistry,
    JoinScheme,
    aggregate,
    execute_join,
    fingerprint,
)

REPORT_ID = "MR_2026_11"
REVENUE = "M_REVENUE"
REFUND = "M_REFUND"

REVENUE_SCHEME_OLD = "JOIN_REVENUE_OLD"
REVENUE_SCHEME_NEW = "JOIN_REVENUE_NEW"
REFUND_SCHEME = "JOIN_REFUND"


# ============================================================
# 固定时钟 + 测试数据
# ============================================================

class FixedClock:
    def __init__(self) -> None:
        self.ticks = 0

    def __call__(self) -> str:
        self.ticks += 1
        return f"2026-11-24T10:{self.ticks:02d}:00"


@pytest.fixture
def clock():
    return FixedClock()


@pytest.fixture
def registry(clock):
    return BindingRegistry(clock=clock)


@pytest.fixture
def schemes():
    """三份关联方案：旧收入关系、更正后的新收入关系、退款关系。"""
    return {
        REVENUE_SCHEME_OLD: JoinScheme(
            scheme_id=REVENUE_SCHEME_OLD,
            primary_dataset="revenue_orders",
            join_dataset="settlement_old",
            join_keys=["order_id"],
            field_mapping={"settle_no": "settle_no", "signed": "signed"},
        ),
        REVENUE_SCHEME_NEW: JoinScheme(
            scheme_id=REVENUE_SCHEME_NEW,
            primary_dataset="revenue_orders",
            join_dataset="settlement_new",
            join_keys=["order_id"],
            field_mapping={"settle_no": "settle_no", "signed": "signed"},
        ),
        REFUND_SCHEME: JoinScheme(
            scheme_id=REFUND_SCHEME,
            primary_dataset="refund_orders",
            join_dataset="refund_receipt",
            join_keys=["refund_id"],
            field_mapping={"receipt_no": "receipt_no", "checked": "checked"},
        ),
    }


@pytest.fixture
def datasets_v1():
    """首期数据：旧收入关系关联后 amount 合计 60；退款关系关联后 amount 合计 15。

    收入表与退款表都有 amount 字段，用来验证同名字段必须各用各的明细。
    """
    return {
        # 收入主表（业务订单）
        "revenue_orders": [
            {"order_id": "o1", "amount": 20},
            {"order_id": "o2", "amount": 40},
            {"order_id": "o3", "amount": 10},  # 旧结算未签收，关联不上
        ],
        # 旧结算关系：只签收了 o1/o2
        "settlement_old": [
            {"order_id": "o1", "settle_no": "S1", "signed": True},
            {"order_id": "o2", "settle_no": "S2", "signed": True},
        ],
        # 退款主表
        "refund_orders": [
            {"refund_id": "r1", "amount": 5},
            {"refund_id": "r2", "amount": 10},
            {"refund_id": "r3", "amount": 8},  # 无退款回执，关联不上
        ],
        # 退款回执
        "refund_receipt": [
            {"refund_id": "r1", "receipt_no": "RC1", "checked": True},
            {"refund_id": "r2", "receipt_no": "RC2", "checked": True},
        ],
    }


@pytest.fixture
def datasets_v2(datasets_v1):
    """当期变化：新收入关系把 o3 也纳入签收（另一份合法关系，对应 70），
    同时主表被重新保存（覆盖写），模拟「排在最后的主表数据」。
    """
    data = copy.deepcopy(datasets_v1)
    data["settlement_new"] = [
        {"order_id": "o1", "settle_no": "S1-N", "signed": True},
        {"order_id": "o2", "settle_no": "S2-N", "signed": True},
        {"order_id": "o3", "settle_no": "S3-N", "signed": True},
    ]
    return data


def _bind_metrics(registry, schemes):
    """登记收入、退款两项指标各自独立的报告数据集绑定。"""
    registry.open_report(REPORT_ID, "2026年11月月报", "2026-11")
    revenue_binding = registry.register_binding(
        report_id=REPORT_ID,
        metric_id=REVENUE,
        metric_name="收入金额",
        field_name="amount",
        scheme=schemes[REVENUE_SCHEME_OLD],
    )
    refund_binding = registry.register_binding(
        report_id=REPORT_ID,
        metric_id=REFUND,
        metric_name="退款金额",
        field_name="amount",
        scheme=schemes[REFUND_SCHEME],
    )
    return revenue_binding, refund_binding


# ============================================================
# 1. 绑定保存关联方案与真实结果版本
# ============================================================

class TestBindingPersistence:
    def test_binding_stores_scheme_and_result_version(self, registry, schemes, datasets_v1):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)

        revenue_version = registry.run_join(
            revenue_binding.binding_id, datasets_v1, verify=True
        )
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)

        reloaded_revenue = registry.find_binding(REPORT_ID, REVENUE)
        assert reloaded_revenue is not None
        # 绑定里持久化了关联方案
        assert reloaded_revenue.scheme.scheme_id == REVENUE_SCHEME_OLD
        assert reloaded_revenue.scheme.primary_dataset == "revenue_orders"
        # 绑定里持久化了真实结果版本，且为已核验当前版本
        assert reloaded_revenue.current_version_id == revenue_version.version_id
        assert reloaded_revenue.status == BINDING_VERIFIED
        assert reloaded_revenue.current_version().verified is True
        # 真实关联行被冻结在版本里
        assert reloaded_revenue.current_version().row_count == 2
        assert reloaded_revenue.current_version().row_fingerprint == fingerprint(
            reloaded_revenue.current_version().rows
        )

    def test_each_metric_holds_own_binding(self, registry, schemes, datasets_v1):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=True)
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)

        assert revenue_binding.binding_id != refund_binding.binding_id
        assert (
            registry.find_binding(REPORT_ID, REVENUE).scheme.primary_dataset
            == "revenue_orders"
        )
        assert (
            registry.find_binding(REPORT_ID, REFUND).scheme.primary_dataset
            == "refund_orders"
        )


# ============================================================
# 2. 按绑定取各自明细聚合：60 和 15，同名 amount 不串用
# ============================================================

class TestReportCalculation:
    def test_revenue_60_refund_15_from_own_verified_details(
        self, registry, schemes, datasets_v1
    ):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=True)
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)

        snapshots = registry.calculate_report(REPORT_ID)
        assert set(snapshots) == {REVENUE, REFUND}
        # 收入用收入关联明细：20 + 40 = 60
        assert snapshots[REVENUE].value == 60
        # 退款用退款关联明细：5 + 10 = 15
        assert snapshots[REFUND].value == 15

    def test_same_named_amount_field_uses_separate_details(
        self, registry, schemes, datasets_v1
    ):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        revenue_version = registry.run_join(
            revenue_binding.binding_id, datasets_v1, verify=True
        )
        refund_version = registry.run_join(
            refund_binding.binding_id, datasets_v1, verify=True
        )

        revenue_rows = revenue_version.rows
        refund_rows = refund_version.rows
        # 两份明细都含 amount，但来源行完全不同
        assert {row["order_id"] for row in revenue_rows} == {"o1", "o2"}
        assert {row["refund_id"] for row in refund_rows} == {"r1", "r2"}
        assert aggregate(revenue_rows, "amount") == 60
        assert aggregate(refund_rows, "amount") == 15
        # 若错误地串用明细，金额必然对不上
        assert aggregate(revenue_rows, "amount") != aggregate(refund_rows, "amount")

    def test_calculation_requires_verified_binding(self, registry, schemes, datasets_v1):
        revenue_binding, _ = _bind_metrics(registry, schemes)
        # 只跑关联但不核验
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=False)
        with pytest.raises(BindingError):
            registry.calculate_report(REPORT_ID)


# ============================================================
# 3+4. 新收入关系 70；主表最后保存也不能覆盖任何绑定
# ============================================================

class TestCorrectionAndOverwriteProtection:
    def test_legal_new_revenue_relation_yields_70(
        self, registry, schemes, datasets_v1, datasets_v2
    ):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=True)
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)

        # 采用已核验的新收入绑定
        new_version = registry.submit_correction(
            revenue_binding.binding_id,
            schemes[REVENUE_SCHEME_NEW],
            datasets_v2,
            verified=True,
        )
        snapshots = registry.calculate_report(REPORT_ID)
        # 新关系纳入 o3：20 + 40 + 10 = 70；退款不受影响仍为 15
        assert snapshots[REVENUE].value == 70
        assert snapshots[REFUND].value == 15
        assert snapshots[REVENUE].version_id == new_version.version_id
        assert snapshots[REFUND].binding_id == refund_binding.binding_id

    def test_unverified_correction_cannot_replace_usable_binding(
        self, registry, schemes, datasets_v1, datasets_v2
    ):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        old_version = registry.run_join(
            revenue_binding.binding_id, datasets_v1, verify=True
        )
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)

        # 未核验更正：版本保留，但当前可用绑定仍是旧版本
        pending_version = registry.submit_correction(
            revenue_binding.binding_id,
            schemes[REVENUE_SCHEME_NEW],
            datasets_v2,
            verified=False,
        )
        assert pending_version.verified is False
        binding = registry.find_binding(REPORT_ID, REVENUE)
        assert binding.status == BINDING_CORRECTING
        assert binding.current_version_id == old_version.version_id

        snapshots = registry.calculate_report(REPORT_ID)
        # 未经核验的更正不得替换可用绑定：仍按旧绑定报 60 / 15
        assert snapshots[REVENUE].value == 60
        assert snapshots[REFUND].value == 15

        # 核验后才允许切换到 70
        registry.verify_version(binding.binding_id, pending_version.version_id)
        snapshots = registry.calculate_report(REPORT_ID)
        assert snapshots[REVENUE].value == 70
        assert snapshots[REFUND].value == 15

    def test_latest_saved_master_table_cannot_overwrite_bindings(
        self, registry, schemes, datasets_v1
    ):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=True)
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)

        original_revenue_rows = copy.deepcopy(
            registry.find_binding(REPORT_ID, REVENUE).current_version().rows
        )
        # 模拟主表被最后一次保存整体覆盖（金额被改写、行被替换）
        datasets_v1["revenue_orders"] = [
            {"order_id": "oX", "amount": 999},
            {"order_id": "oY", "amount": 888},
        ]
        datasets_v1["refund_orders"] = [
            {"refund_id": "rZ", "amount": 777},
        ]

        snapshots = registry.calculate_report(REPORT_ID)
        # 已核验明细是冻结快照，主表覆盖不影响报告
        assert snapshots[REVENUE].value == 60
        assert snapshots[REFUND].value == 15
        frozen_rows = registry.find_binding(
            REPORT_ID, REVENUE
        ).current_version().rows
        assert frozen_rows == original_revenue_rows


# ============================================================
# 5. 旧绑定 vs 已核验新绑定：60/15 与 70/15 两种报法
# ============================================================

class TestConflictBetweenCorrectionAndSignoff:
    def test_continue_with_old_binding_reports_60_15(
        self, registry, schemes, datasets_v1, datasets_v2
    ):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=True)
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)

        # 已领用旧绑定；确认前发生收入关系更正（未核验）+ 当期签收
        registry.submit_correction(
            revenue_binding.binding_id,
            schemes[REVENUE_SCHEME_NEW],
            datasets_v2,
            verified=False,
        )
        snapshots = registry.calculate_report(REPORT_ID)
        # 继续按旧绑定：60 和 15
        assert snapshots[REVENUE].value == 60
        assert snapshots[REFUND].value == 15

    def test_adopt_verified_new_binding_reports_70_15(
        self, registry, schemes, datasets_v1, datasets_v2
    ):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=True)
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)

        pending = registry.submit_correction(
            revenue_binding.binding_id,
            schemes[REVENUE_SCHEME_NEW],
            datasets_v2,
            verified=False,
        )
        # 更正通过核验后采用新绑定
        registry.verify_version(revenue_binding.binding_id, pending.version_id)
        snapshots = registry.calculate_report(REPORT_ID)
        # 采用已核验新绑定：70 和 15
        assert snapshots[REVENUE].value == 70
        assert snapshots[REFUND].value == 15


# ============================================================
# 6+7. 一起确认、并发只写一次、旧报告按历史行算回原值
# ============================================================

class TestMonthlyConfirmation:
    def test_two_metrics_confirmed_together_and_frozen(
        self, registry, schemes, datasets_v1
    ):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=True)
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)

        report, written = registry.confirm_report(REPORT_ID)
        assert written is True
        assert report.status == "已确认"
        # 同一绑定版本的两项结果一起落库
        assert set(report.snapshots) == {REVENUE, REFUND}
        assert report.snapshots[REVENUE].value == 60
        assert report.snapshots[REFUND].value == 15

    def test_confirmation_is_atomic_when_binding_missing(
        self, registry, schemes, datasets_v1
    ):
        revenue_binding, _ = _bind_metrics(registry, schemes)
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=True)
        # 退款绑定未核验 -> 整体确认失败，报告保持草稿
        with pytest.raises(BindingError):
            registry.confirm_report(REPORT_ID)
        assert registry.get_report(REPORT_ID).status == "草稿"
        assert registry.get_report(REPORT_ID).snapshots == {}

    def test_concurrent_confirmation_writes_once(self, registry, schemes, datasets_v1):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=True)
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)

        outcomes: list[tuple[bool, str]] = []

        def worker() -> None:
            report, written = registry.confirm_report(REPORT_ID)
            outcomes.append((written, report.confirmed_at))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # 只有一次真正写入，其余全部幂等返回
        assert sum(1 for written, _ in outcomes if written) == 1
        assert sum(1 for written, _ in outcomes if not written) == 7
        report = registry.get_report(REPORT_ID)
        assert report.confirmations == 1
        assert len({confirmed_at for _, confirmed_at in outcomes}) == 1

    def test_reentering_confirmed_report_keeps_historical_values(
        self, registry, schemes, datasets_v1, datasets_v2
    ):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=True)
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)
        registry.confirm_report(REPORT_ID)

        # 确认之后再发生收入更正并核验、主表被覆盖
        pending = registry.submit_correction(
            revenue_binding.binding_id,
            schemes[REVENUE_SCHEME_NEW],
            datasets_v2,
            verified=False,
        )
        registry.verify_version(revenue_binding.binding_id, pending.version_id)
        datasets_v2["revenue_orders"] = [{"order_id": "oZ", "amount": 9999}]

        # 重新进入报告：读取冻结快照，不被新绑定/最新主表覆盖
        snapshots = registry.calculate_report(REPORT_ID)
        assert snapshots[REVENUE].value == 60
        assert snapshots[REFUND].value == 15
        # 旧报告仍能按实际历史行算回原值
        assert registry.archived_report_values(REPORT_ID) == {
            REVENUE: 60,
            REFUND: 15,
        }

    def test_historical_and_current_reports_coexist(
        self, registry, schemes, datasets_v1, datasets_v2
    ):
        """旧月报（60/15）冻结后，新月报采用新绑定报 70/15，互不覆盖。"""
        # 旧月报
        old_revenue, old_refund = _bind_metrics(registry, schemes)
        registry.run_join(old_revenue.binding_id, datasets_v1, verify=True)
        registry.run_join(old_refund.binding_id, datasets_v1, verify=True)
        registry.confirm_report(REPORT_ID)

        # 新月报复用同一指标的绑定关系
        new_report = "MR_2026_12"
        registry.open_report(new_report, "2026年12月月报", "2026-12")
        new_revenue = registry.register_binding(
            report_id=new_report,
            metric_id=REVENUE,
            metric_name="收入金额",
            field_name="amount",
            scheme=schemes[REVENUE_SCHEME_OLD],
        )
        new_refund = registry.register_binding(
            report_id=new_report,
            metric_id=REFUND,
            metric_name="退款金额",
            field_name="amount",
            scheme=schemes[REFUND_SCHEME],
        )
        registry.run_join(new_revenue.binding_id, datasets_v2, verify=True)
        registry.run_join(new_refund.binding_id, datasets_v2, verify=True)
        # 新月报采用已核验新收入关系
        pending = registry.submit_correction(
            new_revenue.binding_id,
            schemes[REVENUE_SCHEME_NEW],
            datasets_v2,
            verified=False,
        )
        registry.verify_version(new_revenue.binding_id, pending.version_id)
        registry.confirm_report(new_report)

        # 历史金额保持 60/15，当期报告为 70/15
        assert registry.archived_report_values(REPORT_ID) == {
            REVENUE: 60,
            REFUND: 15,
        }
        current = registry.calculate_report(new_report)
        assert current[REVENUE].value == 70
        assert current[REFUND].value == 15


# ============================================================
# 8. 持久化重载 + 交错收入更正的端到端链路
# ============================================================

class TestPersistenceReloadAndInterleave:
    def test_reload_two_bindings_and_recompute_with_interleaved_correction(
        self, registry, schemes, datasets_v1, datasets_v2
    ):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=True)
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)
        registry.confirm_report(REPORT_ID)
        confirmed_at = registry.get_report(REPORT_ID).confirmed_at

        # 存入后重新读取：用持久化载荷重建一个全新的注册中心实例
        reloaded = BindingRegistry.from_dict(copy.deepcopy(registry.to_dict()))

        # 重新读到两个绑定关系，方案与已核验版本都在
        reloaded_revenue = reloaded.find_binding(REPORT_ID, REVENUE)
        reloaded_refund = reloaded.find_binding(REPORT_ID, REFUND)
        assert reloaded_revenue.scheme.scheme_id == REVENUE_SCHEME_OLD
        assert reloaded_refund.scheme.scheme_id == REFUND_SCHEME
        assert reloaded_revenue.current_version().verified is True
        assert reloaded_refund.current_version().verified is True

        # 正式报告明细：重载后重新进入报告仍为冻结的 60 / 15
        frozen = reloaded.calculate_report(REPORT_ID)
        assert frozen[REVENUE].value == 60
        assert frozen[REFUND].value == 15
        assert reloaded.get_report(REPORT_ID).confirmed_at == confirmed_at

        # 交错执行：真实关联（退款重算）与收入更正穿插进行
        refund_re_run = reloaded.run_join(
            reloaded_refund.binding_id, datasets_v2, verify=True
        )
        pending_revenue = reloaded.submit_correction(
            reloaded_revenue.binding_id,
            schemes[REVENUE_SCHEME_NEW],
            datasets_v2,
            verified=False,
        )
        # 退款再次真实关联，读数仍为 15（另一个指标的真实读数）
        assert aggregate(refund_re_run.rows, "amount") == 15
        # 未核验更正不能影响已确认的正式报告
        assert reloaded.calculate_report(REPORT_ID)[REVENUE].value == 60

        # 核验收入更正后，历史报告仍按历史行；新算口径为 70
        reloaded.verify_version(reloaded_revenue.binding_id, pending_revenue.version_id)
        assert reloaded.archived_report_values(REPORT_ID) == {
            REVENUE: 60,
            REFUND: 15,
        }
        latest_revenue_version = reloaded_revenue.current_version()
        assert aggregate(latest_revenue_version.rows, "amount") == 70

        # 再次持久化重载，正式报告明细、历史金额保持不变
        reloaded_again = BindingRegistry.from_dict(copy.deepcopy(reloaded.to_dict()))
        assert reloaded_again.archived_report_values(REPORT_ID) == {
            REVENUE: 60,
            REFUND: 15,
        }
        assert reloaded_again.calculate_report(REPORT_ID)[REFUND].value == 15
        # 指纹校验仍然通过，实际关联行未被篡改
        revenue_snapshot = reloaded_again.calculate_report(REPORT_ID)[REVENUE]
        assert revenue_snapshot.row_fingerprint == fingerprint(
            revenue_snapshot.detail_rows
        )

    def test_tampered_historical_rows_are_detected_on_reload(
        self, registry, schemes, datasets_v1
    ):
        revenue_binding, refund_binding = _bind_metrics(registry, schemes)
        registry.run_join(revenue_binding.binding_id, datasets_v1, verify=True)
        registry.run_join(refund_binding.binding_id, datasets_v1, verify=True)

        payload = copy.deepcopy(registry.to_dict())
        # 篡改持久化的实际关联行但保留旧指纹
        for binding_payload in payload["bindings"]:
            if binding_payload["metric_id"] == REVENUE:
                binding_payload["versions"][0]["rows"][0]["amount"] = 9999

        tampered = BindingRegistry.from_dict(payload)
        with pytest.raises(BindingError):
            tampered.calculate_report(REPORT_ID)


# ============================================================
# 纯函数确定性
# ============================================================

class TestPureFunctions:
    def test_execute_join_is_deterministic_and_isolated(self, datasets_v1, schemes):
        scheme = schemes[REVENUE_SCHEME_OLD]
        rows_a = execute_join(
            datasets_v1["revenue_orders"],
            datasets_v1["settlement_old"],
            scheme.join_keys,
            scheme.field_mapping,
        )
        rows_b = execute_join(
            datasets_v1["revenue_orders"],
            datasets_v1["settlement_old"],
            scheme.join_keys,
            scheme.field_mapping,
        )
        assert rows_a == rows_b
        # 深拷贝隔离：修改入参不影响已产出的明细
        datasets_v1["revenue_orders"][0]["amount"] = 9999
        assert rows_a[0]["amount"] == 20

    def test_fingerprint_stable(self, datasets_v1):
        rows = datasets_v1["refund_receipt"]
        assert fingerprint(rows) == fingerprint(copy.deepcopy(rows))
        changed = copy.deepcopy(rows)
        changed[0]["receipt_no"] = "TAMPERED"
        assert fingerprint(rows) != fingerprint(changed)

    def test_draft_binding_initial_state(self, registry, schemes):
        revenue_binding, _ = _bind_metrics(registry, schemes)
        assert revenue_binding.status == BINDING_DRAFT
        assert revenue_binding.current_version_id is None
