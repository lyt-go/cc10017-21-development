"""死信处理接口：维护死信记录，并生成可追溯的死信重放映射数据。

动作接口保持原有「加入处置、标记重放、标记丢弃」；重放映射相关接口负责：
解析订单死信、规划上游分区到下游转换节点的映射、中断续跑、导入导出与打包下载。
"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse

from app.schemas import ActionPayload, ActionResult, PageResult
from app.services.dead_letter import DISPLAY_FIELDS, DeadLetterService
from app.services.dead_letter_replay import MAPPING_FIELDS, DeadLetterReplayService

router = APIRouter(prefix="/api/dead_letter", tags=["死信处理"])

service = DeadLetterService()
replay_service = DeadLetterReplayService()

STATUSES = ["草稿", "待处置", "已重放", "已丢弃"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in DISPLAY_FIELDS and value.strip()
    }


def mapping_filters(request: Request) -> dict[str, str]:
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in MAPPING_FIELDS and value.strip()
    }


def csv_response(text: str, filename: str) -> StreamingResponse:
    return StreamingResponse(
        iter([text]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按记录编号检索"),
    status: str | None = Query(default=None, description="草稿、待处置、已重放、已丢弃"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、记录编号关键字与状态过滤死信处理列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


# ── 重放映射：静态路径必须声明在 /{entry_id} 之前 ──────────────────────────

@router.get("/replay/batches")
def list_replay_batches() -> dict:
    """重放批次列表：包含中断位置与已确认数，便于恢复时只处理未完成项。"""
    return {"items": replay_service.list_batches()}


@router.get("/replay/mappings", response_model=PageResult[dict])
def list_replay_mappings(
    request: Request,
    version: int | None = Query(default=None, description="只看某一版本的映射"),
    include_inactive: bool = Query(default=False, description="是否包含已失效版本"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=100, ge=1, le=1000),
) -> PageResult[dict]:
    """读取死信重放映射数据：保留上游分区、下游转换节点与主重放标记。"""
    items, total = replay_service.list_mappings(
        filters=mapping_filters(request), version=version,
        include_inactive=include_inactive, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/replay/check")
def check_replay(batch_id: int | None = Query(default=None)) -> dict:
    """校验映射与血缘边成对写入，并确认每条死信只有一条主重放边。"""
    return replay_service.check(batch_id)


@router.post("/replay/generate", response_model=ActionResult)
def generate_replay_mappings(payload: dict) -> ActionResult:
    """解析订单死信并生成重放映射；处理路线决定血缘边集合与数据版本。

    payload:
      route: 「首个节点接收」或「完整映射形成」
      dead_letter_ids: 可选，限定记录编号
      interrupt_after: 可选，模拟在第 N 项后中断
      force: 可选，按新路线重新生成（旧版本映射与边失效）
    """
    route = str(payload.get("route") or "首个节点接收")
    raw_ids = payload.get("dead_letter_ids") or payload.get("ids") or []
    ids = [str(value) for value in raw_ids] if isinstance(raw_ids, list) else [str(raw_ids)]
    interrupt_after = payload.get("interrupt_after")
    try:
        interrupt_value = None if interrupt_after in (None, "") else int(interrupt_after)
    except (TypeError, ValueError):
        return ActionResult(ok=False, message="中断位置必须是整数")
    result = replay_service.generate(
        topic_code=str(payload.get("topic_code") or "TOPI-0001"),
        route=route, dead_letter_ids=ids or None,
        interrupt_after=interrupt_value, force=bool(payload.get("force")),
    )
    entries = result.get("映射") or []
    return ActionResult(
        ok=bool(result.get("ok")),
        message=str(result.get("message", "")),
        entry={**result, "映射": entries},
    )


@router.post("/replay/resume", response_model=ActionResult)
def resume_replay_mappings(payload: dict) -> ActionResult:
    """恢复中断的重放映射：保留已确认项，只补齐尚未完成的映射与边。"""
    batch_id = payload.get("batch_id")
    interrupt_after = payload.get("interrupt_after")
    try:
        batch_value = None if batch_id in (None, "") else int(batch_id)
        interrupt_value = None if interrupt_after in (None, "") else int(interrupt_after)
    except (TypeError, ValueError):
        return ActionResult(ok=False, message="批次编号与中断位置必须是整数")
    result = replay_service.resume(batch_value, interrupt_after=interrupt_value)
    return ActionResult(ok=bool(result.get("ok")), message=str(result.get("message", "")), entry=result)


@router.get("/replay/export")
def export_replay_mappings(
    request: Request,
    version: int | None = Query(default=None),
    include_inactive: bool = Query(default=False),
) -> StreamingResponse:
    """导出当前有效版本的重放映射清单（同一业务口径）。"""
    text = replay_service.export_mappings_csv(
        filters=mapping_filters(request), version=version, include_inactive=include_inactive)
    return csv_response(text, "dead_letter_replay_mappings.csv")


@router.post("/replay/import", response_model=ActionResult)
async def import_replay_mappings(
    request: Request,
    route: str = Query(default="首个节点接收"),
) -> ActionResult:
    """上传重放清单（CSV 或打包的 zip），沿用与页面一致的解析、校验与成对写入规则。"""
    body = await request.body()
    if not body:
        return ActionResult(ok=False, message="上传文件为空")
    result = replay_service.import_bytes(body, route=route)
    return ActionResult(
        ok=bool(result.get("ok")), message=str(result.get("message", "")),
        entry={k: v for k, v in result.items() if k != "映射"} | {"映射数": len(result.get("映射") or [])},
    )


@router.get("/package")
def download_package() -> Response:
    """打包下载死信清单、重放映射、血缘边与批次清单，便于离线核对。"""
    return Response(
        content=replay_service.package_bytes(), media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="dead_letter_replay_package.zip"'})


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV，附带主重放去向与解析结果。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    header = ["id", *DISPLAY_FIELDS, "status"]
    writer.writerow(header)
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in DISPLAY_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return csv_response(output.getvalue(), "dead_letter.csv")


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条死信记录明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"死信记录 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条死信记录，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="死信记录已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条死信记录执行加入处置、标记重放、标记丢弃；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
