"""流式血缘 DAG 拓扑接口：返回节点、连线图结构，以及共享的死信重放映射。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.stream_lineage import StreamLineageService

router = APIRouter(prefix="/api/stream_lineage", tags=["流式血缘"])
service = StreamLineageService()
LIST_FIELDS = ["记录编号", "上游主题", "下游表名", "转换节点", "字段映射", "采集时间", "延迟偏移", "链路版本"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]
MAPPING_FIELDS = ["映射编号", "版本", "死信编号", "原始主题", "原始分区", "转换节点", "下游表名", "主重放", "映射状态"]


def safe_cell(value: Any) -> str:
    """CSV 注入防护：以公式符号开头的单元格加前缀。"""
    text = str(value) if value is not None else ""
    return "'" + text if text.startswith(("=", "+", "-", "@")) else text


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据。"""
    return service.get_graph()


@router.get("/replay-mappings")
def get_replay_mappings(version: int | None = Query(default=None)) -> dict[str, Any]:
    """另一个业务入口：与死信处理页共享同一份死信重放映射数据。"""
    return service.replay_mappings(version)


@router.get("/export")
def export_entries() -> StreamingResponse:
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    nodes, edges = service.raw_data()
    writer.writerow(["type", "id", "name", "from", "to", "label", "status", "version", "death_letter", "primary"])
    for node in nodes:
        writer.writerow(["node", safe_cell(node.get("id", "")), safe_cell(node.get("name", "")),
                         "", "", "", safe_cell(node.get("status", "")), safe_cell(node.get("version", "")), "", ""])
    for edge in edges:
        is_replay = "死信编号" in edge
        writer.writerow([
            "edge", "", "", safe_cell(edge.get("from", "")), safe_cell(edge.get("to", "")),
            safe_cell(edge.get("label", "")), "", safe_cell(edge.get("版本", "")),
            safe_cell(edge.get("死信编号", "")) if is_replay else "",
            ("是" if edge.get("主重放") else "否") if is_replay else "",
        ])
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=stream_lineage.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"血缘记录 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="血缘记录已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
