"""洞察工坊 · 报告数据集绑定领域

解决「报告缺少按指标保存实际关联数据来源入口」的问题：

* 每个指标单独保存自己的**关联方案**（主数据集、关联数据集、关联键、字段映射）
  和**真实关联结果版本**（已核验的关联明细快照），即「报告数据集绑定」。
* 报告计算严格按绑定取每项指标**自己实际关联后的明细**再聚合；
  收入与退款即使字段都叫 ``amount``，也各用各的已核验明细，互不串用。
* 绑定只认「已核验」版本：未经核验的更正不得替换当前可用绑定。
* 月报确认时把同一绑定版本下所有指标的结果**一起**落库并冻结，
  并发确认同一次月报只写一次（幂等 + 互斥）。
* 重新进入报告时读取持久化的指标绑定并核对实际关联行，
  不被最新一次保存的主表或其他指标覆盖；旧报告始终能按历史行算回原值。

本模块只依赖标准库，所有计算都是确定性纯函数，可直接用于测试。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
from dataclasses import dataclass, field
from typing import Any, Callable

# 绑定生命周期状态
BINDING_DRAFT = "草稿"
BINDING_VERIFIED = "已核验"
BINDING_CORRECTING = "更正中"
BINDING_ARCHIVED = "已归档"

# 月报状态
REPORT_DRAFT = "草稿"
REPORT_CONFIRMED = "已确认"

# 合法的绑定状态流转
BINDING_STATUS_ORDER = [BINDING_DRAFT, BINDING_VERIFIED, BINDING_CORRECTING, BINDING_ARCHIVED]


# ============================================================
# 确定性工具函数
# ============================================================

def stable_json(payload: Any) -> str:
    """对任意可 JSON 化结构生成稳定字符串，作为版本指纹输入。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fingerprint(rows: list[dict[str, Any]]) -> str:
    """对关联明细行计算确定性指纹，用于核验「实际关联行」是否变化。"""
    digest = hashlib.sha256(stable_json(rows).encode("utf-8")).hexdigest()
    return digest[:16]


def aggregate(rows: list[dict[str, Any]], field_name: str, method: str = "sum") -> float:
    """按指标口径对明细行聚合。

    与 ``MetricDef.compute`` 保持一致的确定性规则；这里显式接收字段名，
    保证收入（amount）与退款（amount）即使字段同名，也是对各自明细分别求和。
    """
    values: list[float] = []
    for row in rows:
        value = row.get(field_name)
        if value is None or value == "":
            continue
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            continue
    if not values:
        return 0.0
    if method == "sum":
        return round(sum(values), 6)
    if method == "count":
        return float(len(values))
    if method == "avg":
        return round(sum(values) / len(values), 6)
    if method == "max":
        return max(values)
    if method == "min":
        return min(values)
    raise ValueError(f"不支持的聚合方式：{method}")


def execute_join(
    primary_rows: list[dict[str, Any]],
    join_rows: list[dict[str, Any]],
    join_keys: list[str],
    field_mapping: dict[str, str],
    join_type: str = "inner",
) -> list[dict[str, Any]]:
    """执行确定性关联：主表每行按关联键匹配，映射字段写入关联后的明细。

    与 ``models.DatasetJoin.execute_join`` 同口径，但不依赖枚举，
    且对输入做深拷贝，调用方后续修改主表不会污染已经核验的明细。
    """
    index: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in join_rows:
        key = tuple(str(row.get(k, "")) for k in join_keys)
        index.setdefault(key, row)

    result: list[dict[str, Any]] = []
    for primary in primary_rows:
        key = tuple(str(primary.get(k, "")) for k in join_keys)
        matched = index.get(key)
        if matched is not None:
            merged = copy.deepcopy(primary)
            for source_field, target_field in field_mapping.items():
                merged[target_field] = matched.get(source_field, "")
            result.append(merged)
        elif join_type == "left":
            merged = copy.deepcopy(primary)
            for _source_field, target_field in field_mapping.items():
                merged.setdefault(target_field, "")
            result.append(merged)
    return result


# ============================================================
# 领域对象
# ============================================================

@dataclass
class JoinScheme:
    """关联方案：描述一项指标的明细要把哪两张表、按什么键关联起来。"""
    scheme_id: str
    primary_dataset: str
    join_dataset: str
    join_keys: list[str] = field(default_factory=list)
    join_type: str = "inner"
    field_mapping: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scheme_id": self.scheme_id,
            "primary_dataset": self.primary_dataset,
            "join_dataset": self.join_dataset,
            "join_keys": list(self.join_keys),
            "join_type": self.join_type,
            "field_mapping": dict(self.field_mapping),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "JoinScheme":
        return cls(
            scheme_id=payload["scheme_id"],
            primary_dataset=payload["primary_dataset"],
            join_dataset=payload["join_dataset"],
            join_keys=list(payload.get("join_keys", [])),
            join_type=payload.get("join_type", "inner"),
            field_mapping=dict(payload.get("field_mapping", {})),
        )


@dataclass
class JoinResultVersion:
    """真实关联结果版本：一次关联执行后冻结下来的已核验明细。

    ``rows`` 是深拷贝快照，主表后续被覆盖保存也不会改动这里的历史行。
    """
    version_id: str
    scheme_id: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    row_count: int = 0
    row_fingerprint: str = ""
    verified: bool = False
    created_at: str = ""

    def __post_init__(self) -> None:
        self.row_count = len(self.rows)
        if not self.row_fingerprint:
            self.row_fingerprint = fingerprint(self.rows)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version_id": self.version_id,
            "scheme_id": self.scheme_id,
            "rows": copy.deepcopy(self.rows),
            "row_count": self.row_count,
            "row_fingerprint": self.row_fingerprint,
            "verified": self.verified,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "JoinResultVersion":
        version = cls(
            version_id=payload["version_id"],
            scheme_id=payload["scheme_id"],
            rows=copy.deepcopy(payload.get("rows", [])),
            row_fingerprint=payload.get("row_fingerprint", ""),
            verified=bool(payload.get("verified", False)),
            created_at=payload.get("created_at", ""),
        )
        return version


@dataclass
class MetricBinding:
    """报告数据集绑定：一项指标所用的关联方案 + 真实结果版本。

    绑定按指标隔离：收入指标和退款指标各自持有独立的方案与版本，
    哪怕两边的明细字段都叫 ``amount`` 也不会互相覆盖。
    """
    binding_id: str
    report_id: str
    metric_id: str
    metric_name: str
    field_name: str
    aggregation: str = "sum"
    scheme: JoinScheme | None = None
    current_version_id: str | None = None
    status: str = BINDING_DRAFT
    versions: list[JoinResultVersion] = field(default_factory=list)
    # 同一指标的绑定版本号自增，旧版本永久保留用于历史报告回溯
    sequence: int = 0

    def get_version(self, version_id: str | None) -> JoinResultVersion | None:
        if version_id is None:
            return None
        for version in self.versions:
            if version.version_id == version_id:
                return version
        return None

    def current_version(self) -> JoinResultVersion | None:
        return self.get_version(self.current_version_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "binding_id": self.binding_id,
            "report_id": self.report_id,
            "metric_id": self.metric_id,
            "metric_name": self.metric_name,
            "field_name": self.field_name,
            "aggregation": self.aggregation,
            "scheme": self.scheme.to_dict() if self.scheme else None,
            "current_version_id": self.current_version_id,
            "status": self.status,
            "sequence": self.sequence,
            "versions": [version.to_dict() for version in self.versions],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MetricBinding":
        scheme_payload = payload.get("scheme")
        return cls(
            binding_id=payload["binding_id"],
            report_id=payload["report_id"],
            metric_id=payload["metric_id"],
            metric_name=payload["metric_name"],
            field_name=payload["field_name"],
            aggregation=payload.get("aggregation", "sum"),
            scheme=JoinScheme.from_dict(scheme_payload) if scheme_payload else None,
            current_version_id=payload.get("current_version_id"),
            status=payload.get("status", BINDING_DRAFT),
            sequence=payload.get("sequence", 0),
            versions=[JoinResultVersion.from_dict(item) for item in payload.get("versions", [])],
        )


@dataclass
class MetricSnapshot:
    """月报确认时单项指标的冻结读数：绑定版本、实际关联行与聚合值一起落库。"""
    metric_id: str
    metric_name: str
    binding_id: str
    scheme_id: str
    version_id: str
    row_fingerprint: str
    row_count: int
    field_name: str
    value: float
    detail_rows: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric_id": self.metric_id,
            "metric_name": self.metric_name,
            "binding_id": self.binding_id,
            "scheme_id": self.scheme_id,
            "version_id": self.version_id,
            "row_fingerprint": self.row_fingerprint,
            "row_count": self.row_count,
            "field_name": self.field_name,
            "value": self.value,
            "detail_rows": copy.deepcopy(self.detail_rows),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MetricSnapshot":
        return cls(
            metric_id=payload["metric_id"],
            metric_name=payload["metric_name"],
            binding_id=payload["binding_id"],
            scheme_id=payload["scheme_id"],
            version_id=payload["version_id"],
            row_fingerprint=payload["row_fingerprint"],
            row_count=payload["row_count"],
            field_name=payload["field_name"],
            value=payload["value"],
            detail_rows=copy.deepcopy(payload.get("detail_rows", [])),
        )


@dataclass
class MonthlyReport:
    """一次月报：草稿期按当前已核验绑定计算，确认后整体冻结。"""
    report_id: str
    report_name: str
    period: str
    metric_ids: list[str] = field(default_factory=list)
    status: str = REPORT_DRAFT
    # 确认后冻结的指标读数（同一绑定版本的两项结果一起确认）
    snapshots: dict[str, MetricSnapshot] = field(default_factory=dict)
    confirmed_at: str = ""
    confirmations: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "report_id": self.report_id,
            "report_name": self.report_name,
            "period": self.period,
            "metric_ids": list(self.metric_ids),
            "status": self.status,
            "confirmed_at": self.confirmed_at,
            "confirmations": self.confirmations,
            "snapshots": {
                metric_id: snapshot.to_dict()
                for metric_id, snapshot in self.snapshots.items()
            },
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MonthlyReport":
        return cls(
            report_id=payload["report_id"],
            report_name=payload["report_name"],
            period=payload["period"],
            metric_ids=list(payload.get("metric_ids", [])),
            status=payload.get("status", REPORT_DRAFT),
            confirmed_at=payload.get("confirmed_at", ""),
            confirmations=payload.get("confirmations", 0),
            snapshots={
                metric_id: MetricSnapshot.from_dict(snapshot_payload)
                for metric_id, snapshot_payload in payload.get("snapshots", {}).items()
            },
        )


# ============================================================
# 绑定注册中心：持久化 + 核验 + 报告计算 + 确认
# ============================================================

class BindingRegistry:
    """报告数据集绑定的内存注册中心（真实项目中替换为数据库仓储）。

    所有写操作都在同一把互斥锁内完成，保证并发确认同一次月报只写一次；
    状态通过 ``to_dict`` / ``from_dict`` 可完整持久化与重载。
    """

    def __init__(self, clock: Callable[[], str] | None = None) -> None:
        self._bindings: dict[str, MetricBinding] = {}
        self._reports: dict[str, MonthlyReport] = {}
        self._lock = threading.RLock()
        self._clock = clock or _default_clock

    # ---------- 持久化 ----------

    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            return {
                "bindings": [
                    binding.to_dict() for binding in self._bindings.values()
                ],
                "reports": [
                    report.to_dict() for report in self._reports.values()
                ],
            }

    @classmethod
    def from_dict(
        cls,
        payload: dict[str, Any],
        clock: Callable[[], str] | None = None,
    ) -> "BindingRegistry":
        registry = cls(clock=clock)
        with registry._lock:
            for item in payload.get("bindings", []):
                binding = MetricBinding.from_dict(item)
                registry._bindings[binding.binding_id] = binding
            for item in payload.get("reports", []):
                report = MonthlyReport.from_dict(item)
                registry._reports[report.report_id] = report
        return registry

    # ---------- 绑定登记 ----------

    def register_binding(
        self,
        *,
        report_id: str,
        metric_id: str,
        metric_name: str,
        field_name: str,
        scheme: JoinScheme,
        aggregation: str = "sum",
    ) -> MetricBinding:
        """为某份报告的某项指标登记独立绑定（同指标重复登记直接返回旧绑定）。"""
        with self._lock:
            for binding in self._bindings.values():
                if binding.report_id == report_id and binding.metric_id == metric_id:
                    return binding
            binding_id = f"BIND_{report_id}_{metric_id}"
            binding = MetricBinding(
                binding_id=binding_id,
                report_id=report_id,
                metric_id=metric_id,
                metric_name=metric_name,
                field_name=field_name,
                aggregation=aggregation,
                scheme=copy.deepcopy(scheme),
            )
            self._bindings[binding_id] = binding
            self._reports.setdefault(
                report_id,
                MonthlyReport(
                    report_id=report_id,
                    report_name=report_id,
                    period="",
                ),
            )
            report = self._reports[report_id]
            if metric_id not in report.metric_ids:
                report.metric_ids.append(metric_id)
            return binding

    def open_report(
        self, report_id: str, report_name: str, period: str
    ) -> MonthlyReport:
        """登记/打开一份月报。"""
        with self._lock:
            report = self._reports.get(report_id)
            if report is None:
                report = MonthlyReport(
                    report_id=report_id,
                    report_name=report_name,
                    period=period,
                )
                self._reports[report_id] = report
            return report

    def get_binding(self, binding_id: str) -> MetricBinding | None:
        return self._bindings.get(binding_id)

    def find_binding(self, report_id: str, metric_id: str) -> MetricBinding | None:
        for binding in self._bindings.values():
            if binding.report_id == report_id and binding.metric_id == metric_id:
                return binding
        return None

    def bindings_for_report(self, report_id: str) -> list[MetricBinding]:
        return [
            binding for binding in self._bindings.values()
            if binding.report_id == report_id
        ]

    def all_bindings(self) -> list[MetricBinding]:
        with self._lock:
            return list(self._bindings.values())

    def get_report(self, report_id: str) -> MonthlyReport | None:
        return self._reports.get(report_id)

    # ---------- 关联执行、核验与更正 ----------

    def run_join(
        self,
        binding_id: str,
        datasets: dict[str, list[dict[str, Any]]],
        *,
        verify: bool = False,
    ) -> JoinResultVersion:
        """按绑定自己的关联方案执行真实关联，产出一个新的结果版本。

        数据始终从绑定持有的方案里取（主数据集名 + 关联数据集名），
        不从外部传入的「当前主表」隐式读取，避免被最后保存的主表覆盖。
        """
        with self._lock:
            binding = self._require_binding(binding_id)
            if binding.scheme is None:
                raise BindingError(f"绑定 {binding_id} 尚未配置关联方案")
            scheme = binding.scheme
            primary_rows = datasets.get(scheme.primary_dataset, [])
            join_rows = datasets.get(scheme.join_dataset, [])
            rows = execute_join(
                primary_rows=primary_rows,
                join_rows=join_rows,
                join_keys=list(scheme.join_keys),
                field_mapping=dict(scheme.field_mapping),
                join_type=scheme.join_type,
            )
            binding.sequence += 1
            version = JoinResultVersion(
                version_id=f"{binding.binding_id}_V{binding.sequence}",
                scheme_id=scheme.scheme_id,
                rows=rows,
                verified=verify,
                created_at=self._clock(),
            )
            binding.versions.append(version)
            if verify:
                self._promote_verified(binding, version)
            return version

    def verify_version(self, binding_id: str, version_id: str) -> MetricBinding:
        """核验某个关联结果版本；只有已核验版本才能成为当前可用绑定。"""
        with self._lock:
            binding = self._require_binding(binding_id)
            version = binding.get_version(version_id)
            if version is None:
                raise BindingError(
                    f"绑定 {binding_id} 不存在结果版本 {version_id}"
                )
            version.verified = True
            self._promote_verified(binding, version)
            return binding

    def submit_correction(
        self,
        binding_id: str,
        new_scheme: JoinScheme,
        datasets: dict[str, list[dict[str, Any]]],
        *,
        verified: bool = False,
    ) -> JoinResultVersion:
        """提交关联方案更正并按新方案跑一次真实关联。

        更正版本默认「未核验」：它会被保留下来，但不会替换当前可用绑定；
        只有 ``verified=True``（或之后显式 ``verify_version``）才会切换。
        """
        with self._lock:
            binding = self._require_binding(binding_id)
            if binding.status == BINDING_ARCHIVED:
                raise BindingError(f"绑定 {binding_id} 已归档，不能再提交更正")
            binding.scheme = copy.deepcopy(new_scheme)
            binding.status = BINDING_CORRECTING
            return self.run_join(binding_id, datasets, verify=verified)

    def mark_status(self, binding_id: str, status: str) -> MetricBinding:
        """在注册中心锁内更新绑定生命周期状态（通用动作入口使用）。"""
        with self._lock:
            binding = self._require_binding(binding_id)
            if status not in BINDING_STATUS_ORDER:
                raise BindingError(f"未知绑定状态：{status}")
            binding.status = status
            return binding

    def _promote_verified(
        self, binding: MetricBinding, version: JoinResultVersion
    ) -> None:
        binding.current_version_id = version.version_id
        binding.status = BINDING_VERIFIED

    # ---------- 报告计算 ----------

    def calculate_report(self, report_id: str) -> dict[str, MetricSnapshot]:
        """草稿期计算：每项指标按**自己当前已核验绑定**的明细独立聚合。

        已确认报告直接返回冻结快照，重新进入报告不会被最新主表或其他指标覆盖。
        """
        with self._lock:
            report = self._require_report(report_id)
            if report.status == REPORT_CONFIRMED:
                return dict(report.snapshots)

            results: dict[str, MetricSnapshot] = {}
            for metric_id in report.metric_ids:
                results[metric_id] = self._snapshot_from_current_binding(
                    report_id, metric_id
                )
            return results

    def _snapshot_from_current_binding(
        self, report_id: str, metric_id: str
    ) -> MetricSnapshot:
        binding = self.find_binding(report_id, metric_id)
        if binding is None:
            raise BindingError(f"报告 {report_id} 的指标 {metric_id} 缺少数据集绑定")
        version = binding.current_version()
        if version is None or not version.verified:
            raise BindingError(
                f"指标 {metric_id} 的绑定 {binding.binding_id} 没有已核验的关联结果版本"
            )
        scheme = binding.scheme
        assert scheme is not None
        # 再次核验实际关联行指纹：持久化重载后行被篡改会立即暴露
        if fingerprint(version.rows) != version.row_fingerprint:
            raise BindingError(
                f"绑定 {binding.binding_id} 版本 {version.version_id} 的实际关联行"
                "与核验指纹不一致"
            )
        value = aggregate(version.rows, binding.field_name, binding.aggregation)
        return MetricSnapshot(
            metric_id=metric_id,
            metric_name=binding.metric_name,
            binding_id=binding.binding_id,
            scheme_id=scheme.scheme_id,
            version_id=version.version_id,
            row_fingerprint=version.row_fingerprint,
            row_count=version.row_count,
            field_name=binding.field_name,
            value=value,
            detail_rows=copy.deepcopy(version.rows),
        )

    # ---------- 月报确认 ----------

    def confirm_report(
        self, report_id: str, *, confirmed_at: str | None = None
    ) -> tuple[MonthlyReport, bool]:
        """确认月报：同一绑定版本的所有指标结果一起冻结落库。

        返回 ``(报告, 是否本次真正写入)``：
        并发/重复确认同一次月报只写一次，后续调用幂等返回已确认报告。
        """
        with self._lock:
            report = self._require_report(report_id)
            if report.status == REPORT_CONFIRMED:
                return report, False

            snapshots: dict[str, MetricSnapshot] = {}
            # 先把所有指标的快照算出来，任何一个缺已核验绑定都会整体失败，
            # 保证「同一绑定版本的两项结果一起确认」，不会落半个报告。
            for metric_id in report.metric_ids:
                snapshots[metric_id] = self._snapshot_from_current_binding(
                    report_id, metric_id
                )
            report.snapshots = snapshots
            report.status = REPORT_CONFIRMED
            report.confirmed_at = confirmed_at or self._clock()
            report.confirmations += 1
            return report, True

    def archived_report_values(
        self, report_id: str
    ) -> dict[str, float]:
        """旧报告重算入口：完全按冻结的历史明细行算回原值。"""
        with self._lock:
            report = self._require_report(report_id)
            values: dict[str, float] = {}
            for metric_id, snapshot in report.snapshots.items():
                # 用快照里冻结的历史行重新聚合，而不是读任何当前主表
                values[metric_id] = aggregate(
                    snapshot.detail_rows, snapshot.field_name, "sum"
                )
            return values

    # ---------- 内部辅助 ----------

    def _require_binding(self, binding_id: str) -> MetricBinding:
        binding = self._bindings.get(binding_id)
        if binding is None:
            raise BindingError(f"报告数据集绑定 {binding_id} 不存在")
        return binding

    def _require_report(self, report_id: str) -> MonthlyReport:
        report = self._reports.get(report_id)
        if report is None:
            raise BindingError(f"月报 {report_id} 不存在")
        return report


class BindingError(Exception):
    """报告数据集绑定领域的业务异常。"""


def _default_clock() -> str:
    from datetime import datetime
    return datetime.now().isoformat(timespec="seconds")
