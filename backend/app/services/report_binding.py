"""报告数据集绑定领域服务。

把 :mod:`app.report_binding` 的绑定注册中心接入通用资源路由：
列表/详情直接读共享 store 中的展示行，绑定、核验、更正、月报确认等
真实业务动作走注册中心，并把最新状态同步回 store 供工作台展示。
"""
from __future__ import annotations

import copy
from typing import Any

from app.report_binding import (
    BINDING_STATUS_ORDER,
    BindingError,
    BindingRegistry,
    JoinScheme,
)
from app.services.base import BaseModuleService
from app.store import store

# 进程级单例注册中心：真实项目里这里是数据库仓储；
# to_dict/from_dict 已保证绑定与月报可以完整持久化、重载。
_registry: BindingRegistry | None = None


def _get_registry() -> BindingRegistry:
    global _registry
    if _registry is None:
        _registry = BindingRegistry()
    return _registry


# 向后兼容的模块级句柄：首次访问时惰性初始化
class _RegistryProxy:
    def __getattr__(self, name: str) -> Any:
        return getattr(_get_registry(), name)


registry = _RegistryProxy()

MODULE = "report_binding"


class ReportBindingService(BaseModuleService):
    MODULE = MODULE
    ENTITY = "报告数据集绑定"
    OPERATION_TERM = "核验"
    LIST_FIELDS = [
        "绑定编号", "报告编号", "指标名称", "关联方案", "结果版本",
        "结果行数", "明细指纹", "绑定状态",
    ]
    REQUIRED_FIELDS = ["绑定编号", "报告编号", "指标名称", "关联方案"]
    STATUS_ORDER = BINDING_STATUS_ORDER
    ACTION_RULES = {
        "提交核验": "已核验",
        "发起更正": "更正中",
        "归档绑定": "已归档",
    }
    NEGATIVE_ACTIONS = []

    # ---------- 注册中心桥接 ----------

    def get_registry(self) -> BindingRegistry:
        return _get_registry()

    def reload_registry(self) -> BindingRegistry:
        """从持久化载荷重建注册中心（模拟「存入后重新读取」）。

        替换模块级单例，保证服务、路由和后续调用读到的都是重载后的实例。
        """
        global _registry
        current = _get_registry()
        _registry = BindingRegistry.from_dict(copy.deepcopy(current.to_dict()))
        self._sync_all()
        return _registry

    def register_binding(
        self,
        *,
        report_id: str,
        report_name: str,
        period: str,
        metric_id: str,
        metric_name: str,
        field_name: str,
        scheme: JoinScheme,
        aggregation: str = "sum",
    ) -> dict[str, Any]:
        registry.open_report(report_id, report_name, period)
        binding = registry.register_binding(
            report_id=report_id,
            metric_id=metric_id,
            metric_name=metric_name,
            field_name=field_name,
            scheme=scheme,
            aggregation=aggregation,
        )
        self._sync_binding(binding)
        return binding.to_dict()

    def run_join(
        self,
        binding_id: str,
        datasets: dict[str, list[dict[str, Any]]],
        *,
        verify: bool = False,
    ) -> dict[str, Any]:
        version = registry.run_join(binding_id, datasets, verify=verify)
        self._sync_binding(registry.get_binding(binding_id))
        return version.to_dict()

    def verify_version(self, binding_id: str, version_id: str) -> dict[str, Any]:
        binding = registry.verify_version(binding_id, version_id)
        self._sync_binding(binding)
        return binding.to_dict()

    def submit_correction(
        self,
        binding_id: str,
        new_scheme: JoinScheme,
        datasets: dict[str, list[dict[str, Any]]],
        *,
        verified: bool = False,
    ) -> dict[str, Any]:
        version = registry.submit_correction(
            binding_id, new_scheme, datasets, verified=verified
        )
        self._sync_binding(registry.get_binding(binding_id))
        return version.to_dict()

    def calculate_report(self, report_id: str) -> dict[str, dict[str, Any]]:
        snapshots = registry.calculate_report(report_id)
        return {
            metric_id: snapshot.to_dict()
            for metric_id, snapshot in snapshots.items()
        }

    def confirm_report(self, report_id: str) -> dict[str, Any]:
        report, written = registry.confirm_report(report_id)
        for binding in registry.bindings_for_report(report_id):
            self._sync_binding(binding)
        return {"report": report.to_dict(), "written": written}

    def archived_report_values(self, report_id: str) -> dict[str, float]:
        return registry.archived_report_values(report_id)

    # ---------- 通用资源路由适配 ----------

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        rows = store.rows(self.MODULE)
        display_row = next((row for row in rows if int(row.get("id", 0)) == entry_id), None)
        if display_row is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        binding_id = str(display_row.get("绑定编号", ""))
        binding = registry.get_binding(binding_id)
        if binding is None:
            return None, f"绑定 {binding_id} 不在注册中心"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        try:
            if action == "提交核验":
                current = binding.current_version()
                if current is None:
                    return None, "绑定还没有关联结果版本，请先执行关联"
                registry.verify_version(binding_id, current.version_id)
            elif action == "发起更正":
                registry.mark_status(binding_id, "更正中")
            elif action == "归档绑定":
                registry.mark_status(binding_id, "已归档")
        except BindingError as exc:
            return None, str(exc)
        self._sync_binding(binding)
        synced = store.find(self.MODULE, entry_id)
        return synced, f"{self.ENTITY}已{action}"

    # ---------- store 展示行同步 ----------

    def _sync_all(self) -> None:
        for binding in registry.all_bindings():
            self._sync_binding(binding)

    def _sync_binding(self, binding: Any) -> None:
        rows = store.rows(self.MODULE)
        existing = next(
            (row for row in rows if row.get("绑定编号") == binding.binding_id),
            None,
        )
        version = binding.current_version()
        values = {
            "绑定编号": binding.binding_id,
            "报告编号": binding.report_id,
            "指标名称": binding.metric_name,
            "关联方案": binding.scheme.scheme_id if binding.scheme else "",
            "结果版本": version.version_id if version else "",
            "结果行数": version.row_count if version else 0,
            "明细指纹": version.row_fingerprint if version else "",
            "绑定状态": binding.status,
        }
        if existing is None:
            entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
            entry.update(values)
            entry["status"] = binding.status
            entry["pending"] = binding.status != BINDING_STATUS_ORDER[-1]
            entry["abnormal"] = binding.status == "更正中"
            rows.append(entry)
        else:
            existing.update(values)
            existing["status"] = binding.status
            existing["pending"] = binding.status != BINDING_STATUS_ORDER[-1]
            existing["abnormal"] = binding.status == "更正中"
