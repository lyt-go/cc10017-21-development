"""流式血缘 DAG 图服务：生成节点、边示例结构与流转规则。

在原有血缘记录基础上，额外读取死信重放映射数据当前有效版本，
把「上游分区 -> 下游转换节点」补成可追溯的边。
"""
from __future__ import annotations

from typing import Any

from app.services.dead_letter_replay import DeadLetterReplayService
from app.store import store

MODULE = "stream_lineage"
REQUIRED_FIELDS = ["记录编号", "上游主题", "下游表名"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用记录": "已停用"}
NEGATIVE_ACTIONS = ["停用记录"]
GRAPH_NODE_SKIP = ("id", "status", "pending", "abnormal", "type", "version")

replay_service = DeadLetterReplayService()


class StreamLineageService:
    def get_graph(self) -> dict[str, Any]:
        rows = store.rows(MODULE)
        mapping_version = replay_service.latest_version()
        nodes: list[dict] = []
        for i, row in enumerate(rows):
            node = {
                "id": int(row.get("id", i + 1)),
                "name": str(row.get("记录编号") or row.get("上游主题") or ""),
                "status": str(row.get("status", STATUS_ORDER[0])),
                "type": "lineage",
                "version": row.get("链路版本", ""),
            }
            node.update({k: row[k] for k in row if k not in GRAPH_NODE_SKIP})
            nodes.append(node)
        edges: list[dict] = []
        for i in range(len(nodes) - 1):
            edges.append({"from": nodes[i]["id"], "to": nodes[i + 1]["id"], "label": "依赖"})
        # 读取死信重放映射数据当前有效版本，建立上游分区到下游转换节点的边；
        # 中断批次只补边，不重复为已确认节点连第二条边。
        seen: set[tuple] = set()
        for edge in replay_service.lineage_replay_edges():
            key = (edge["from"], edge["to"], edge.get("死信编号"), edge.get("版本"))
            if key in seen:
                continue
            seen.add(key)
            edges.append(edge)
        nodes.extend(replay_service.replay_nodes())
        return {"nodes": nodes, "edges": edges, "replay_version": mapping_version}

    def replay_mappings(self, version: int | None = None) -> dict[str, Any]:
        """另一个业务入口共享同一份死信重放映射数据。"""
        items, total = replay_service.list_mappings(version=version, page=1, size=100000)
        return {
            "items": items, "total": total,
            "check": replay_service.check(),
            "version": replay_service.latest_version(),
        }

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        for field in ("转换节点", "字段映射", "采集时间", "延迟偏移", "链路版本"):
            entry[field] = values.get(field, "")
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"血缘记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流式血缘可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"血缘记录已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]

    def export_rows(self) -> list[list[Any]]:
        """导出节点、血缘边与死信重放边；重放边单独标注来源与主重放。"""
        nodes, edges = self.raw_data()
        rows: list[list[Any]] = [
            ["type", "id", "name", "from", "to", "label", "status", "version", "death_letter", "primary"]]
        for node in nodes:
            rows.append(["node", node.get("id", ""), node.get("name", ""), "", "", "",
                         node.get("status", ""), node.get("version", ""), "", ""])
        for edge in edges:
            is_replay = "死信编号" in edge
            rows.append([
                "edge", "", "", edge.get("from", ""), edge.get("to", ""), edge.get("label", ""), "",
                edge.get("版本", ""),
                edge.get("死信编号", "") if is_replay else "",
                ("是" if edge.get("主重放") else "否") if is_replay else "",
            ])
        return rows
