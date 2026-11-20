"""死信处理业务规则：状态流转、字段校验与筛选口径都收在这里。

重放不再是单纯改状态：标记重放时会尽量落到一条主重放映射上，
让页面能说明死信来自哪个上游分区、进入哪个下游转换节点。
"""
from __future__ import annotations

from typing import Any

from app.services.dead_letter_replay import DeadLetterReplayService
from app.store import store

MODULE = "dead_letter"
LIST_FIELDS = ["记录编号", "原始主题", "原始分区", "失败原因", "原始消息体", "重试次数", "入库时间", "处置状态"]
DISPLAY_FIELDS = LIST_FIELDS + ["重放转换节点", "重放下游表名", "重放映射状态", "解析结果"]
REQUIRED_FIELDS = ["记录编号", "原始主题", "原始分区"]
STATUS_ORDER = ["草稿", "待处置", "已重放", "已丢弃"]
ACTION_RULES = {"加入处置": "待处置", "标记重放": "已重放", "标记丢弃": "已丢弃"}
NEGATIVE_ACTIONS = []

replay_service = DeadLetterReplayService()


class DeadLetterService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = [self.decorate(row) for row in store.rows(MODULE)]
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("记录编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        return self.decorate(entry) if entry else None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return self.decorate(entry), []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"死信记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于死信处理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"

        mapping_note = ""
        if action == "标记重放":
            # 先落重放映射再改状态：重放后必须能说明消息进入了哪个下游表。
            result = replay_service.record_replay(entry)
            mapping_note = "；" + str(result.get("message", ""))
            entry["重放映射结果"] = result.get("message", "")

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return self.decorate(entry), f"死信记录已{action}{mapping_note}"

    def decorate(self, row: dict[str, Any]) -> dict[str, Any]:
        """回填主重放映射去向与解析结果，列表和导出共用同一口径。"""
        entry = dict(row)
        code = str(entry.get("记录编号") or "")
        mappings = [m for m in replay_service.active_mappings()
                    if str(m.get("死信编号")) == code]
        primary = next((m for m in mappings if m.get("主重放")), None)
        entry["重放映射数"] = len(mappings)
        entry["重放映射编号"] = primary.get("映射编号") if primary else ""
        entry["重放转换节点"] = primary.get("转换节点") if primary else ""
        entry["重放下游表名"] = primary.get("下游表名") if primary else ""
        entry["重放映射状态"] = primary.get("映射状态") if primary else ""
        entry["解析结果"] = (
            primary.get("解析结果") if primary
            else replay_service.parse_entry(entry).get("summary", "")
        )
        return entry
