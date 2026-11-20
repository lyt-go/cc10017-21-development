"""死信重放映射业务规则：解析死信、规划重放去向、原子写映射与血缘边。

核心约定：
1. 一条死信只允许有一个「主重放」入口，但可以因处理路线不同产生多条候选映射边。
2. 映射记录与「上游分区 -> 下游转换节点」边必须成对写入，任一侧校验失败则两侧都不落库。
3. 生成中断后批次保留已确认项，恢复时只补齐尚未完成的映射，不重写已确认数据。
4. 新路线重新生成会提升版本，旧版本映射与边标记失效，保证任一时刻只有一套有效边集。
"""
from __future__ import annotations

import csv
import io
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from app.store import store

MODULE = "dead_letter"
LINEAGE_MODULE = "stream_lineage"
TOPIC_MODULE = "topic_registry"
MAPPING_TABLE = "dead_letter_replay_mappings"
EDGE_TABLE = "dead_letter_replay_edges"
BATCH_TABLE = "dead_letter_replay_batches"

ORDER_TOPIC = "TOPI-0001"
ROUTE_FIRST = "首个节点接收"
ROUTE_FULL = "完整映射形成"
ROUTE_ALIASES = {
    "首个节点接收": ROUTE_FIRST,
    "first_node": ROUTE_FIRST,
    "首个节点": ROUTE_FIRST,
    "完整映射形成": ROUTE_FULL,
    "full_mapping": ROUTE_FULL,
    "完整映射": ROUTE_FULL,
}

STATUS_CONFIRMED = "已确认"
STATUS_REPLAYED = "已重放"
STATUS_INVALID = "已失效"
ACTIVE_STATUSES = (STATUS_CONFIRMED, STATUS_REPLAYED)

BATCH_RUNNING = "进行中"
BATCH_INTERRUPTED = "已中断"
BATCH_DONE = "已完成"
BATCH_SUPERSEDED = "已作废"

TRUTHY = {"是", "true", "1", "yes", "y"}
MAPPING_FIELDS = [
    "映射编号", "批次编号", "版本", "死信编号", "原始主题", "原始分区",
    "转换节点", "下游表名", "血缘记录编号", "字段映射", "解析结果",
    "主重放", "映射状态", "确认时间",
]
BATCH_FIELDS = [
    "批次编号", "主题编号", "处理路线", "版本", "状态", "计划数",
    "已确认数", "中断位置", "创建时间", "更新时间",
]

_JSON_KEY_RE = re.compile(r"([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:")


def now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")


def csv_cell(value: Any) -> str:
    """单元格转义：仅做公式注入防护，引号交给 csv.writer 统一处理。"""
    text = "" if value is None else str(value)
    if text.startswith(("=", "+", "-", "@")):
        text = "'" + text
    return text


def next_id(table: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(table)), default=0) + 1


def parse_payload(raw: Any) -> dict[str, Any]:
    """解析订单死信消息体，兼容 JSON、单引号与未加引号的键。"""
    text = str(raw or "").strip()
    if not text:
        return {"ok": False, "error": "原始消息体为空", "summary": ""}
    normalized = _JSON_KEY_RE.sub(r'\1"\2":', text.replace("'", '"'))
    try:
        data = json.loads(normalized)
    except json.JSONDecodeError:
        return {"ok": False, "error": "消息体不是合法 JSON", "summary": text[:60]}
    if not isinstance(data, dict):
        return {"ok": False, "error": "消息体不是键值结构", "summary": text[:60]}
    order_id = data.get("order_id")
    if order_id is None or not str(order_id).strip():
        return {"ok": False, "error": "缺少 order_id", "summary": json.dumps(data, ensure_ascii=False)[:60]}
    amount = data.get("amount")
    return {
        "ok": True,
        "order_id": order_id,
        "amount": amount,
        "summary": f"order_id={order_id};amount={amount}",
    }


class DeadLetterReplayService:
    # ───────────────────────────────
    # 查询
    # ───────────────────────────────

    def active_mappings(self) -> list[dict[str, Any]]:
        return [m for m in store.rows(MAPPING_TABLE) if m.get("映射状态") in ACTIVE_STATUSES]

    def active_edges(self) -> list[dict[str, Any]]:
        return [e for e in store.rows(EDGE_TABLE) if e.get("有效", True)]

    def latest_version(self) -> int:
        versions = [
            int(m.get("版本", 0)) for m in store.rows(MAPPING_TABLE)
        ] + [int(b.get("版本", 0)) for b in store.rows(BATCH_TABLE)]
        return max(versions, default=0)

    def list_batches(self) -> list[dict[str, Any]]:
        return [self.strip_internal(b) for b in sorted(
            store.rows(BATCH_TABLE), key=lambda b: int(b.get("id", 0)), reverse=True)]

    def list_mappings(
        self,
        *,
        filters: dict[str, str] | None = None,
        version: int | None = None,
        include_inactive: bool = False,
        page: int = 1,
        size: int = 1000,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(MAPPING_TABLE))
        if version is not None:
            rows = [row for row in rows if int(row.get("版本", 0)) == version]
        if not include_inactive:
            rows = [row for row in rows if row.get("映射状态") in ACTIVE_STATUSES]
        for field, expected in (filters or {}).items():
            if field in ("主重放", "是否主重放"):
                want = str(expected).strip().casefold() in TRUTHY
                rows = [row for row in rows if bool(row.get("主重放")) == want]
                continue
            rows = [row for row in rows
                    if str(expected).casefold() in str(row.get(field, "")).casefold()]
        rows.sort(key=lambda row: (int(row.get("版本", 0)), int(row.get("id", 0))), reverse=True)
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def check(self, batch_id: int | None = None) -> dict[str, Any]:
        """校验中断恢复后的完整性与「一条死信一个主重放边」。"""
        batches = store.rows(BATCH_TABLE)
        batch = None
        if batch_id is not None:
            batch = next((b for b in batches if int(b.get("id", 0)) == batch_id), None)
            if batch is None:
                return {"ok": False, "message": f"重放批次 {batch_id} 不存在"}
        elif batches:
            # 作废批次只保留审计痕迹，检查口径以最近一次有效批次为准。
            usable = [b for b in batches if b.get("状态") != BATCH_SUPERSEDED] or batches
            batch = max(usable, key=lambda b: int(b.get("id", 0)))
        if batch is None:
            return {
                "ok": True, "message": "暂无重放批次", "version": 0,
                "计划": 0, "已确认": 0, "待补齐": 0,
                "primary_ok": True, "atomic_ok": True, "all_confirmed": True,
                "主重放重复": [], "配对异常": [], "缺失项": [],
            }

        version = int(batch.get("版本", 0))
        planned = list(batch.get("_计划项", []))
        current = [m for m in store.rows(MAPPING_TABLE)
                   if int(m.get("版本", 0)) == version and m.get("映射状态") in ACTIVE_STATUSES]
        committed = {(m.get("死信编号"), m.get("转换节点")) for m in current}
        missing = [item for item in planned if (item.get("死信编号"), item.get("转换节点")) not in committed]

        primaries: dict[str, list[dict]] = defaultdict(list)
        for row in self.active_mappings():
            if row.get("主重放"):
                primaries[str(row.get("死信编号"))].append(row)
        duplicated = [
            {"死信编号": code, "主重放映射": [m.get("映射编号") for m in rows]}
            for code, rows in primaries.items() if len(rows) > 1
        ]

        # 成对写入按「所有有效映射与有效边」核对：后续批次补建的主映射也参与校验。
        active_mappings = self.active_mappings()
        active_edges = self.active_edges()
        edge_index = Counter(
            (e.get("死信编号"), e.get("转换节点"), int(e.get("版本", 0)))
            for e in active_edges
        )
        pairing_errors = [
            {"映射编号": m.get("映射编号"), "原因": "缺少配套血缘边"}
            for m in active_mappings
            if edge_index[(m.get("死信编号"), m.get("转换节点"), int(m.get("版本", 0)))] != 1
        ]
        mapping_index = {
            (m.get("死信编号"), m.get("转换节点"), int(m.get("版本", 0))) for m in active_mappings
        }
        pairing_errors.extend(
            {"边": e.get("id"), "原因": "缺少配套重放映射"}
            for e in active_edges
            if (e.get("死信编号"), e.get("转换节点"), int(e.get("版本", 0))) not in mapping_index
        )
        return {
            "ok": True,
            "batch": self.strip_internal(batch),
            "version": version,
            "计划": len(planned),
            "已确认": len(planned) - len(missing),
            "待补齐": len(missing),
            "primary_ok": not duplicated,
            "atomic_ok": not pairing_errors,
            "all_confirmed": not missing,
            "主重放重复": duplicated,
            "配对异常": pairing_errors,
            "缺失项": [
                {"死信编号": i.get("死信编号"), "转换节点": i.get("转换节点")} for i in missing
            ],
        }

    # ───────────────────────────────
    # 生成 / 恢复
    # ───────────────────────────────

    def generate(
        self,
        *,
        topic_code: str = ORDER_TOPIC,
        route: str = ROUTE_FIRST,
        dead_letter_ids: list[str] | None = None,
        interrupt_after: int | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        route = ROUTE_ALIASES.get(str(route).strip(), "")
        if not route:
            return {"ok": False, "message": f"处理路线「{route}」不属于死信重放可执行范围"}
        topic = self._find_topic(topic_code)
        if topic is None:
            return {"ok": False, "message": f"原始主题 {topic_code} 未注册"}

        lines = self._effective_lineage(topic_code)
        if not lines:
            return {"ok": False, "message": f"主题 {topic_code} 未配置已生效的转换节点，无法形成重放映射"}

        targets = self._eligible_dead_letters(topic_code, dead_letter_ids)
        if not targets:
            message = "没有可重放的同主题死信记录"
            if dead_letter_ids:
                requested = {str(value).strip() for value in dead_letter_ids if str(value).strip()}
                discarded = [
                    code for code in sorted(requested)
                    if any(str(dl.get("记录编号")) == code and (
                        dl.get("status") == "已丢弃" or dl.get("处置状态") == "已丢弃")
                        for dl in store.rows(MODULE))
                ]
                if discarded:
                    message = f"记录 {'、'.join(discarded)} 已丢弃，不再建立重放入口"
            return {"ok": True, "message": message, "新增": 0, "版本": self.latest_version()}

        # 同一批死信的可接收节点可能不止一个：换路线意味着重新收口，
        # 旧版本映射与边整体失效，否则两种边集会同时留在当前图里。
        topic_codes = [dl.get("记录编号") for dl in self._eligible_dead_letters(topic_code, None)]
        active_codes = {str(m.get("死信编号")) for m in self.active_mappings()}
        active_route = self._active_route(topic_code)
        route_changed = bool(active_codes & {str(code) for code in topic_codes}) and \
            active_route not in (None, route)
        if not force and not route_changed and active_route == route:
            # 同一路线再次触发即视为续跑：只补未完成项，不另开批次覆盖已确认节点。
            pending = self._interrupted_batch(topic_code, route)
            if pending is not None:
                return self.resume(int(pending.get("id", 0)), interrupt_after=interrupt_after)
        if force or route_changed:
            self._deactivate(topic_codes)

        # 以「死信 + 转换节点」为最小完成单位，部分完成的死信也能补齐剩余节点。
        existing = {
            (str(m.get("死信编号")), str(m.get("转换节点")))
            for m in self.active_mappings()
        }
        targets = [
            dl for dl in targets
            if any((str(dl.get("记录编号")), str(line.get("转换节点"))) not in existing
                   for line in (lines[:1] if route == ROUTE_FIRST else lines))
        ]
        if not targets:
            return {"ok": True, "message": "该路线下的有效重放映射已齐全，仅处理未完成项", "新增": 0,
                    "版本": self.latest_version()}

        planned, skipped = self._plan(targets, lines, route)
        if not planned:
            return {"ok": True, "message": "死信消息体均无法解析，未生成重放映射",
                    "新增": 0, "跳过": skipped, "版本": self.latest_version()}

        version = self.latest_version() + 1
        batch = self._create_batch(topic_code, route, version, planned)
        committed: list[dict[str, Any]] = []
        interrupted = False
        for index, item in enumerate(planned):
            if interrupt_after is not None and index >= max(0, interrupt_after):
                interrupted = True
                break
            committed.append(self._commit(item, version, str(batch.get("批次编号"))))
        self._finish_batch(batch, interrupted, interrupt_after)
        message = (
            f"重放映射写入在第 {len(committed)} 项后中断，已确认映射与血缘边均已保留"
            if interrupted else f"已生成 {len(committed)} 条重放映射并同步血缘边"
        )
        return {
            "ok": True, "message": message, "中断": interrupted,
            "版本": version, "批次": self.strip_internal(batch),
            "新增": len(committed), "跳过": skipped,
            "映射": committed, "批次编号": batch.get("批次编号"),
            "检查": self.check(int(batch.get("id", 0))),
        }

    def resume(self, batch_id: int | None = None, *, interrupt_after: int | None = None) -> dict[str, Any]:
        batches = store.rows(BATCH_TABLE)
        batch = None
        if batch_id is not None:
            batch = next((b for b in batches if int(b.get("id", 0)) == batch_id), None)
            if batch is None:
                return {"ok": False, "message": f"重放批次 {batch_id} 不存在"}
        else:
            interrupted = [b for b in batches if b.get("状态") == BATCH_INTERRUPTED]
            if not interrupted:
                return {"ok": False, "message": "没有可恢复的中断批次",
                        "检查": self.check()}
            batch = max(interrupted, key=lambda b: int(b.get("id", 0)))

        if batch.get("状态") == BATCH_SUPERSEDED:
            return {"ok": False,
                    "message": "该批次已被新的处理路线取代，旧路线不再补齐",
                    "保留已确认": 0, "本次补齐": 0,
                    "版本": batch.get("版本"), "批次": self.strip_internal(batch),
                    "检查": self.check()}
        if batch.get("状态") == BATCH_DONE:
            return {"ok": True, "message": "该批次已完成，无需恢复",
                    "保留已确认": int(batch.get("已确认数", 0)), "本次补齐": 0,
                    "版本": batch.get("版本"), "批次": self.strip_internal(batch),
                    "检查": self.check(int(batch.get("id", 0)))}

        version = int(batch.get("版本", 0))
        planned = list(batch.get("_计划项", []))
        committed_keys = {
            (m.get("死信编号"), m.get("转换节点"))
            for m in store.rows(MAPPING_TABLE)
            if int(m.get("版本", 0)) == version and m.get("映射状态") in ACTIVE_STATUSES
        }
        preserved = len(committed_keys)
        added: list[dict[str, Any]] = []
        interrupted = False
        for item in planned:
            key = (item.get("死信编号"), item.get("转换节点"))
            if key in committed_keys:
                continue
            if interrupt_after is not None and len(added) >= max(0, interrupt_after):
                interrupted = True
                break
            added.append(self._commit(item, version, str(batch.get("批次编号"))))
            committed_keys.add(key)
        self._finish_batch(batch, interrupted, interrupt_after)
        # 已确认数按批次实际落库映射统计，避免沿用中断时的旧计数
        batch["已确认数"] = len([
            m for m in store.rows(MAPPING_TABLE)
            if m.get("批次编号") == batch.get("批次编号") and m.get("映射状态") in ACTIVE_STATUSES
        ])
        batch["更新时间"] = now_text()
        message = (
            f"恢复再次中断，保留已确认 {preserved} 条，本次补齐 {len(added)} 条"
            if interrupted else f"已恢复批次，保留已确认 {preserved} 条，补齐 {len(added)} 条"
        )
        return {
            "ok": True, "message": message, "中断": interrupted,
            "保留已确认": preserved, "本次补齐": len(added),
            "版本": version, "批次": self.strip_internal(batch),
            "映射": added, "检查": self.check(int(batch.get("id", 0))),
        }

    def record_replay(self, entry: dict[str, Any]) -> dict[str, Any]:
        """「标记重放」时补齐主重放映射，让重放动作能说明消息进入哪个下游表。"""
        dead_letter_code = str(entry.get("记录编号") or "")
        primary = self._active_primary(dead_letter_code)
        if primary:
            primary["映射状态"] = STATUS_REPLAYED
            entry.update({
                "重放映射编号": primary.get("映射编号"),
                "重放转换节点": primary.get("转换节点"),
                "重放下游表名": primary.get("下游表名"),
                "重放映射状态": STATUS_REPLAYED,
            })
            return {"ok": True, "message": "沿用已确认的主重放映射", "映射": primary}

        topic_code = str(entry.get("原始主题") or "")
        lines = self._effective_lineage(topic_code)
        if not lines:
            entry["重放映射结果"] = "未找到生效的下游转换节点，重放去向未建立"
            return {"ok": False, "message": "该主题未配置已生效的转换节点，未建立重放映射"}

        parsed = self._parse_for_topic(topic_code, entry.get("原始消息体"))
        node = lines[0]
        item = {
            "死信编号": dead_letter_code,
            "原始主题": topic_code,
            "原始分区": entry.get("原始分区", ""),
            "转换节点": node.get("转换节点"),
            "下游表名": node.get("下游表名"),
            "血缘记录编号": node.get("记录编号"),
            "字段映射": node.get("字段映射", ""),
            "解析结果": parsed.get("summary", ""),
            "主重放": True,
        }
        version = self.latest_version() + 1
        batch = self._create_batch(topic_code, ROUTE_FIRST, version, [item])
        mapping = self._commit(item, version, str(batch.get("批次编号")), status=STATUS_REPLAYED)
        self._finish_batch(batch, False, None)
        entry.update({
            "重放映射编号": mapping.get("映射编号"),
            "重放转换节点": mapping.get("转换节点"),
            "重放下游表名": mapping.get("下游表名"),
            "重放映射状态": STATUS_REPLAYED,
        })
        return {"ok": True, "message": "已建立主重放映射", "映射": mapping}

    # ───────────────────────────────
    # 导入 / 导出
    # ───────────────────────────────

    def import_csv(self, text: str, *, route: str = ROUTE_FIRST) -> dict[str, Any]:
        text = text.lstrip("﻿")
        reader = csv.DictReader(io.StringIO(text))
        fields = [str(name).strip() for name in (reader.fieldnames or [])]
        if not fields:
            return {"ok": False, "message": "上传文件为空或缺少表头"}
        rows = list(reader)
        if "死信编号" in fields and "转换节点" in fields:
            return self._import_mapping_rows(rows, route)
        if "记录编号" in fields:
            codes = [str(row.get("记录编号") or "").strip() for row in rows]
            codes = [code for code in codes if code]
            if not codes:
                return {"ok": False, "message": "上传文件未包含有效的记录编号"}
            return self.generate(dead_letter_ids=codes, route=route)
        if "from" in fields and "to" in fields:
            return self._import_edge_rows(rows, route)
        return {"ok": False, "message": "上传文件需包含「死信编号+转换节点」或「记录编号」列"}

    def import_bytes(self, data: bytes, *, route: str = ROUTE_FIRST) -> dict[str, Any]:
        """上传既可以是 CSV，也可以是打包下载的 zip，解析与写入口径完全一致。"""
        if data[:2] == b"PK":
            import zipfile
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as archive:
                    names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
                    preferred = next(
                        (n for n in names if n in ("dead_letter_replay.csv", "stream_lineage_replay_edges.csv")),
                        names[0] if names else None,
                    )
                    if preferred is None:
                        return {"ok": False, "message": "压缩包内没有可用的 CSV 清单"}
                    data = archive.read(preferred)
            except zipfile.BadZipFile:
                return {"ok": False, "message": "上传的压缩包无法读取"}
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError:
            return {"ok": False, "message": "上传文件需为 UTF-8 编码的 CSV"}
        return self.import_csv(text, route=route)

    def _import_edge_rows(self, rows: list[dict[str, Any]], route: str) -> dict[str, Any]:
        """上传血缘边清单：仅保留死信重放边，按同口径回填为映射行。"""
        converted: list[dict[str, Any]] = []
        for row in rows:
            if str(row.get("label") or "").strip() != "死信重放":
                continue
            target = str(row.get("to") or "").strip()
            lineage = next((item for item in store.rows(LINEAGE_MODULE)
                            if str(item.get("id")) == target), None)
            converted.append({
                "死信编号": row.get("死信编号") or row.get("death_letter"),
                "原始分区": (str(row.get("from") or "").split(":")[-1]),
                "转换节点": lineage.get("转换节点") if lineage else "",
                "主重放": row.get("主重放") or row.get("primary"),
            })
        if not converted:
            return {"ok": False, "message": "上传文件没有死信重放边"}
        return self._import_mapping_rows(converted, route)

    def export_mappings_csv(
        self,
        *,
        filters: dict[str, str] | None = None,
        version: int | None = None,
        include_inactive: bool = False,
    ) -> str:
        items, _ = self.list_mappings(
            filters=filters, version=version,
            include_inactive=include_inactive, page=1, size=1_000_000)
        output = io.StringIO()
        output.write("﻿")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(MAPPING_FIELDS)
        for item in items:
            writer.writerow([csv_cell(item.get(field, "")) for field in MAPPING_FIELDS])
        return output.getvalue()

    def export_edges_csv(self) -> str:
        output = io.StringIO()
        output.write("﻿")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["版本", "死信编号", "from", "to", "label", "主重放", "有效"])
        for edge in self.active_edges():
            writer.writerow([
                csv_cell(edge.get("版本", "")), csv_cell(edge.get("死信编号", "")),
                csv_cell(edge.get("from", "")), csv_cell(edge.get("to", "")),
                csv_cell(edge.get("label", "")), csv_cell("是" if edge.get("主重放") else "否"),
                csv_cell("是" if edge.get("有效", True) else "否"),
            ])
        return output.getvalue()

    def export_dead_letters_csv(self) -> str:
        output = io.StringIO()
        output.write("﻿")
        writer = csv.writer(output, lineterminator="\n")
        fields = ["记录编号", "原始主题", "原始分区", "失败原因", "原始消息体",
                  "重试次数", "入库时间", "处置状态", "status"]
        writer.writerow(fields)
        for row in store.rows(MODULE):
            writer.writerow([csv_cell(row.get(field, "")) for field in fields])
        return output.getvalue()

    def package_bytes(self) -> bytes:
        import zipfile
        manifest = {
            "generated_at": now_text(),
            "latest_version": self.latest_version(),
            "mapping_count": len(self.active_mappings()),
            "edge_count": len(self.active_edges()),
            "batches": self.list_batches(),
        }
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("dead_letter.csv", self.export_dead_letters_csv())
            archive.writestr("dead_letter_replay.csv", self.export_mappings_csv())
            archive.writestr("stream_lineage_replay_edges.csv", self.export_edges_csv())
            archive.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        return buffer.getvalue()

    # ───────────────────────────────
    # 节点标识与图数据
    # ───────────────────────────────

    @staticmethod
    def partition_node_id(topic_code: str, partition: Any) -> str:
        return f"replay-partition:{topic_code}:{partition}"

    @staticmethod
    def transform_node_id(lineage_id: Any) -> str:
        return f"replay-transform:{lineage_id}"

    def lineage_replay_edges(self) -> list[dict[str, Any]]:
        """供流式血缘读取当前有效版本的映射，生成上游分区到下游转换节点的边。"""
        edges: list[dict[str, Any]] = []
        for mapping in self.active_mappings():
            lineage_id = mapping.get("血缘记录编号")
            lineage = self._find_lineage(lineage_id, mapping.get("转换节点"), mapping.get("原始主题"))
            target = int(lineage.get("id", 0)) if lineage else self.transform_node_id(lineage_id)
            edges.append({
                "from": self.partition_node_id(mapping.get("原始主题"), mapping.get("原始分区")),
                "to": target,
                "label": "死信重放",
                "死信编号": mapping.get("死信编号"),
                "映射编号": mapping.get("映射编号"),
                "转换节点": mapping.get("转换节点"),
                "下游表名": mapping.get("下游表名"),
                "主重放": bool(mapping.get("主重放")),
                "版本": mapping.get("版本"),
            })
        return edges

    def replay_nodes(self) -> list[dict[str, Any]]:
        nodes: list[dict[str, Any]] = []
        seen: set[str] = set()
        for mapping in self.active_mappings():
            partition_id = self.partition_node_id(mapping.get("原始主题"), mapping.get("原始分区"))
            if partition_id not in seen:
                seen.add(partition_id)
                nodes.append({
                    "id": partition_id,
                    "name": f"{mapping.get('原始主题')}/分区{mapping.get('原始分区')}",
                    "type": "replay_partition",
                    "status": "待重放",
                    "原始主题": mapping.get("原始主题"),
                    "原始分区": mapping.get("原始分区"),
                    "版本": mapping.get("版本"),
                })
        return nodes

    # ───────────────────────────────
    # 内部辅助
    # ───────────────────────────────

    def strip_internal(self, row: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in row.items() if not str(key).startswith("_")}

    def _find_topic(self, topic_code: str) -> dict[str, Any] | None:
        return next((row for row in store.rows(TOPIC_MODULE)
                     if row.get("主题编号") == topic_code), None)

    def _is_order_topic(self, topic_code: str) -> bool:
        if topic_code == ORDER_TOPIC:
            return True
        topic = self._find_topic(topic_code)
        return bool(topic and "订单" in str(topic.get("主题名称", "")))

    def parse_entry(self, entry: dict[str, Any]) -> dict[str, Any]:
        """解析单条死信记录的消息体，供页面与导出说明数据进入哪个下游转换节点。"""
        return self._parse_for_topic(str(entry.get("原始主题") or ""), entry.get("原始消息体"))

    def _parse_for_topic(self, topic_code: str, raw: Any) -> dict[str, Any]:
        if self._is_order_topic(topic_code):
            return parse_payload(raw)
        text = str(raw or "").strip()
        return {"ok": bool(text), "summary": text[:60], "error": None if text else "原始消息体为空"}

    def _effective_lineage(self, topic_code: str) -> list[dict[str, Any]]:
        lines: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in sorted(store.rows(LINEAGE_MODULE), key=lambda r: int(r.get("id", 0))):
            if row.get("上游主题") != topic_code or row.get("status") != "已生效":
                continue
            node = str(row.get("转换节点") or "").strip()
            if not node or node in seen:
                continue
            seen.add(node)
            lines.append(row)
        return lines

    def _eligible_dead_letters(self, topic_code: str, ids: list[str] | None) -> list[dict[str, Any]]:
        wanted = {str(value).strip() for value in (ids or []) if str(value).strip()}
        rows: list[dict[str, Any]] = []
        for row in store.rows(MODULE):
            if row.get("原始主题") != topic_code:
                continue
            if wanted and str(row.get("记录编号")) not in wanted:
                continue
            if row.get("status") == "已丢弃" or row.get("处置状态") == "已丢弃":
                continue
            rows.append(row)
        return rows

    def _plan(
        self,
        targets: list[dict[str, Any]],
        lines: list[dict[str, Any]],
        route: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        selected = lines[:1] if route == ROUTE_FIRST else lines
        planned: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for dead_letter in targets:
            topic_code = str(dead_letter.get("原始主题") or "")
            parsed = self._parse_for_topic(topic_code, dead_letter.get("原始消息体"))
            if not parsed.get("ok"):
                skipped.append({
                    "死信编号": dead_letter.get("记录编号"),
                    "原因": parsed.get("error") or "消息体无法解析",
                })
                continue
            for index, node in enumerate(selected):
                planned.append({
                    "死信编号": dead_letter.get("记录编号"),
                    "原始主题": topic_code,
                    "原始分区": dead_letter.get("原始分区", ""),
                    "转换节点": node.get("转换节点"),
                    "下游表名": node.get("下游表名"),
                    "血缘记录编号": node.get("记录编号"),
                    "字段映射": node.get("字段映射", ""),
                    "解析结果": parsed.get("summary", ""),
                    "主重放": index == 0,
                })
        return planned, skipped

    def _create_batch(
        self, topic_code: str, route: str, version: int, planned: list[dict[str, Any]]
    ) -> dict[str, Any]:
        batch_id = next_id(BATCH_TABLE)
        batch = {
            "id": batch_id,
            "批次编号": f"DLRB-{batch_id:04d}",
            "主题编号": topic_code,
            "处理路线": route,
            "版本": version,
            "状态": BATCH_RUNNING,
            "计划数": len(planned),
            "已确认数": 0,
            "中断位置": None,
            "创建时间": now_text(),
            "更新时间": now_text(),
            "_计划项": [dict(item) for item in planned],
            "_计划死信": sorted({str(item.get("死信编号")) for item in planned}),
        }
        store.rows(BATCH_TABLE).append(batch)
        return batch

    def _finish_batch(
        self,
        batch: dict[str, Any],
        interrupted: bool,
        interrupt_after: int | None,
    ) -> None:
        """收口批次状态；已确认数始终以批次实际落库映射为准。"""
        batch["状态"] = BATCH_INTERRUPTED if interrupted else BATCH_DONE
        batch["中断位置"] = interrupt_after if interrupted else None
        batch["已确认数"] = len([
            m for m in store.rows(MAPPING_TABLE)
            if m.get("批次编号") == batch.get("批次编号")
            and m.get("映射状态") in ACTIVE_STATUSES
        ])
        batch["更新时间"] = now_text()

    def _commit(
        self,
        item: dict[str, Any],
        version: int,
        batch_no: str,
        *,
        status: str = STATUS_CONFIRMED,
    ) -> dict[str, Any]:
        """映射与血缘边成对写入：先完成全部校验，再连续落两份数据。"""
        topic_code = str(item.get("原始主题") or "")
        node_code = str(item.get("转换节点") or "")
        lineage = self._find_lineage(item.get("血缘记录编号"), node_code, topic_code)
        if lineage is None or lineage.get("status") != "已生效":
            raise ValueError(f"转换节点「{node_code}」在主题 {topic_code} 下不存在或未生效")

        primary = bool(item.get("主重放"))
        if primary and self._active_primary(str(item.get("死信编号")), exclude_batch=batch_no):
            primary = False

        mapping_id = next_id(MAPPING_TABLE)
        created = {
            "id": mapping_id,
            "映射编号": f"DLMP-{mapping_id:04d}",
            "批次编号": batch_no,
            "版本": version,
            "死信编号": item.get("死信编号"),
            "原始主题": topic_code,
            "原始分区": item.get("原始分区", ""),
            "转换节点": node_code,
            "下游表名": item.get("下游表名") or lineage.get("下游表名"),
            "血缘记录编号": lineage.get("记录编号"),
            "字段映射": item.get("字段映射") or lineage.get("字段映射", ""),
            "解析结果": item.get("解析结果", ""),
            "主重放": primary,
            "映射状态": status,
            "确认时间": now_text(),
        }
        edge_id = next_id(EDGE_TABLE)
        edge = {
            "id": edge_id,
            "批次编号": batch_no,
            "版本": version,
            "死信编号": item.get("死信编号"),
            "from": self.partition_node_id(topic_code, item.get("原始分区", "")),
            "to": int(lineage.get("id", 0)),
            "label": "死信重放",
            "转换节点": node_code,
            "下游表名": created["下游表名"],
            "主重放": primary,
            "有效": True,
        }
        # 两侧数据成对写入；任一侧异常时回滚已追加的一侧。
        store.rows(MAPPING_TABLE).append(created)
        try:
            store.rows(EDGE_TABLE).append(edge)
        except Exception:
            store.rows(MAPPING_TABLE).remove(created)
            raise
        return created

    def _active_primary(self, dead_letter_code: str, *, exclude_batch: str | None = None) -> dict[str, Any] | None:
        for row in self.active_mappings():
            if str(row.get("死信编号")) != dead_letter_code or not row.get("主重放"):
                continue
            if exclude_batch and row.get("批次编号") == exclude_batch:
                continue
            return row
        return None

    def _interrupted_batch(self, topic_code: str, route: str) -> dict[str, Any] | None:
        candidates = [
            b for b in store.rows(BATCH_TABLE)
            if b.get("状态") == BATCH_INTERRUPTED
            and str(b.get("主题编号")) == topic_code
            and str(b.get("处理路线")) == route
        ]
        return max(candidates, key=lambda b: int(b.get("id", 0))) if candidates else None

    def _active_route(self, topic_code: str) -> str | None:
        """当前有效映射对应的处理路线：取生成这些映射的最高版本批次。"""
        versions = {
            int(m.get("版本", 0)) for m in self.active_mappings()
            if str(m.get("原始主题")) == topic_code
        }
        candidates = [
            b for b in store.rows(BATCH_TABLE)
            if int(b.get("版本", 0)) in versions
            and str(b.get("主题编号")) == topic_code
            and b.get("状态") != BATCH_SUPERSEDED
        ]
        if not candidates:
            return None
        latest = max(candidates, key=lambda b: (int(b.get("版本", 0)), int(b.get("id", 0))))
        return str(latest.get("处理路线") or "")

    def _deactivate(self, dead_letter_codes: list[str]) -> None:
        """旧版本映射、边与未完成批次一起收口，避免换路线后两套边集并存。"""
        wanted = {str(code) for code in dead_letter_codes}
        for batch in store.rows(BATCH_TABLE):
            if batch.get("状态") == BATCH_DONE:
                continue
            planned = {str(code) for code in batch.get("_计划死信", [])}
            if planned & wanted:
                batch["状态"] = BATCH_SUPERSEDED
                batch["更新时间"] = now_text()
        for mapping in store.rows(MAPPING_TABLE):
            if str(mapping.get("死信编号")) in wanted and mapping.get("映射状态") in ACTIVE_STATUSES:
                mapping["映射状态"] = STATUS_INVALID
                mapping["主重放"] = False
        for edge in store.rows(EDGE_TABLE):
            if str(edge.get("死信编号")) in wanted and edge.get("有效", True):
                edge["有效"] = False

    def _find_lineage(
        self, lineage_code: Any, node_code: Any, topic_code: str
    ) -> dict[str, Any] | None:
        rows = store.rows(LINEAGE_MODULE)
        if lineage_code:
            found = next((row for row in rows if row.get("记录编号") == lineage_code), None)
            if found:
                return found
        return next((row for row in rows
                     if row.get("上游主题") == topic_code and row.get("转换节点") == node_code), None)

    def _import_mapping_rows(self, rows: list[dict[str, Any]], route: str) -> dict[str, Any]:
        route = ROUTE_ALIASES.get(str(route).strip(), ROUTE_FIRST)
        planned: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        primary_seen: set[str] = set()
        for row in rows:
            dead_letter_code = str(row.get("死信编号") or "").strip()
            node_code = str(row.get("转换节点") or "").strip()
            if not dead_letter_code or not node_code:
                skipped.append({"行": row, "原因": "缺少死信编号或转换节点"})
                continue
            dead_letter = next((dl for dl in store.rows(MODULE)
                                if str(dl.get("记录编号")) == dead_letter_code), None)
            if dead_letter is None:
                skipped.append({"死信编号": dead_letter_code, "原因": "死信记录不存在"})
                continue
            if dead_letter.get("status") == "已丢弃" or dead_letter.get("处置状态") == "已丢弃":
                skipped.append({"死信编号": dead_letter_code, "原因": "死信已丢弃，不再建立重放入口"})
                continue
            topic_code = str(dead_letter.get("原始主题") or "")
            partition = str(row.get("原始分区") or dead_letter.get("原始分区", ""))
            if partition != str(dead_letter.get("原始分区", "")):
                skipped.append({"死信编号": dead_letter_code, "原因": "原始分区与死信记录不一致"})
                continue
            lineage = self._find_lineage(row.get("血缘记录编号"), node_code, topic_code)
            if lineage is None or lineage.get("status") != "已生效":
                skipped.append({"死信编号": dead_letter_code, "原因": f"转换节点「{node_code}」未生效"})
                continue
            key = (dead_letter_code, node_code)
            if key in seen:
                skipped.append({"死信编号": dead_letter_code, "原因": "上传清单内重复行"})
                continue
            seen.add(key)
            if any(str(m.get("死信编号")) == dead_letter_code
                   and m.get("转换节点") == node_code and m.get("映射状态") in ACTIVE_STATUSES
                   for m in store.rows(MAPPING_TABLE)):
                skipped.append({"死信编号": dead_letter_code, "原因": "映射已存在，仅处理未完成项"})
                continue
            requested_primary = str(row.get("主重放") or "").strip().casefold() in TRUTHY
            primary = requested_primary and dead_letter_code not in primary_seen
            if primary:
                primary_seen.add(dead_letter_code)
            parsed = self._parse_for_topic(topic_code, dead_letter.get("原始消息体"))
            planned.append({
                "死信编号": dead_letter_code,
                "原始主题": topic_code,
                "原始分区": partition,
                "转换节点": node_code,
                "下游表名": lineage.get("下游表名"),
                "血缘记录编号": lineage.get("记录编号"),
                "字段映射": lineage.get("字段映射", ""),
                "解析结果": parsed.get("summary", ""),
                "主重放": primary,
            })

        # 没有任何行声明主重放时，按「首个节点接收」口径给首行补主入口
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in planned:
            grouped[str(item["死信编号"])].append(item)
        for code, items in grouped.items():
            if code not in primary_seen and items:
                items[0]["主重放"] = True
                primary_seen.add(code)

        if not planned:
            return {"ok": False, "message": "上传清单没有可写入的死信重放映射",
                    "新增": 0, "跳过": skipped, "版本": self.latest_version()}

        version = self.latest_version() + 1
        batch = self._create_batch(planned[0]["原始主题"], route, version, planned)
        committed: list[dict[str, Any]] = []
        for item in planned:
            try:
                committed.append(self._commit(item, version, str(batch.get("批次编号"))))
            except ValueError as error:
                skipped.append({"死信编号": item.get("死信编号"), "原因": str(error)})
        self._finish_batch(batch, False, None)
        return {
            "ok": bool(committed),
            "message": f"上传清单已写入 {len(committed)} 条重放映射，跳过 {len(skipped)} 条",
            "版本": version, "批次": self.strip_internal(batch),
            "新增": len(committed), "跳过": skipped, "映射": committed,
            "检查": self.check(int(batch.get("id", 0))),
        }
