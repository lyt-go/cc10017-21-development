"""死信重放映射测试：解析、路线取舍、原子写边、中断续跑与跨入口共享。"""
from __future__ import annotations

import io
import unittest
import zipfile

from app.services.dead_letter import DeadLetterService
from app.services.dead_letter_replay import (
    EDGE_TABLE, MAPPING_TABLE, ROUTE_FIRST, ROUTE_FULL, DeadLetterReplayService,
)
from app.services.stream_lineage import StreamLineageService
from app.store import store


class ReplayTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.service = DeadLetterReplayService()
        self.dead_letter = DeadLetterService()
        self.lineage = StreamLineageService()
        for table in (MAPPING_TABLE, EDGE_TABLE, "dead_letter_replay_batches"):
            store.rows(table).clear()

    def replay_edges(self) -> list[dict]:
        return [e for e in self.lineage.get_graph()["edges"] if e.get("label") == "死信重放"]


class TestReplayParse(ReplayTestBase):
    def test_order_dead_letter_payload_is_parsed(self) -> None:
        entry = store.find("dead_letter", 1)
        parsed = self.service.parse_entry(entry)
        self.assertTrue(parsed["ok"])
        self.assertEqual(parsed["order_id"], 123)

    def test_unparsable_order_letter_is_skipped(self) -> None:
        store.rows("dead_letter").append({
            "id": 99, "status": "待处置", "pending": True, "abnormal": True,
            "记录编号": "DEAD-0099", "原始主题": "TOPI-0001", "原始分区": "0",
            "失败原因": "格式错误", "原始消息体": "not-a-json", "重试次数": "0",
            "入库时间": "2026-11-16T09:00:00", "处置状态": "待分析",
        })
        result = self.service.generate(dead_letter_ids=["DEAD-0099"])
        self.assertTrue(result["ok"])
        self.assertEqual(result["新增"], 0)
        self.assertEqual(result["跳过"][0]["死信编号"], "DEAD-0099")


class TestReplayRoutes(ReplayTestBase):
    def test_route_switch_requires_no_force_and_retires_old_batch(self) -> None:
        self.service.generate(route=ROUTE_FULL, interrupt_after=2)
        switched = self.service.generate(route=ROUTE_FIRST)
        self.assertTrue(switched["ok"])
        self.assertEqual(switched["新增"], 1)
        self.assertEqual(len(self.service.active_mappings()), 1)
        self.assertEqual(len(self.service.active_edges()), 1)
        stale = self.service.resume(1)
        self.assertFalse(stale["ok"])
        self.assertIn("取代", stale["message"])
        self.assertEqual(len(self.service.active_mappings()), 1)

    def test_route_changes_edge_set_and_version(self) -> None:
        first = self.service.generate(route=ROUTE_FIRST)
        self.assertTrue(first["ok"])
        first_edges = self.replay_edges()
        self.assertEqual(len(first_edges), 1)
        self.assertEqual(first_edges[0]["to"], 1)

        full = self.service.generate(route=ROUTE_FULL, force=True)
        self.assertTrue(full["ok"])
        self.assertGreater(full["版本"], first["版本"])
        full_edges = self.replay_edges()
        self.assertEqual(len(full_edges), 3)
        # 版本切换后旧版本边失效，当前图只保留新版本边集
        self.assertFalse(any(edge.get("版本") == first["版本"] for edge in full_edges))
        self.assertEqual(self.service.check()["primary_ok"], True)

    def test_single_primary_entry_per_dead_letter(self) -> None:
        self.service.generate(route=ROUTE_FULL)
        mappings, _ = self.service.list_mappings()
        primaries = [m for m in mappings if m["主重放"]]
        self.assertEqual(len(primaries), 1)
        self.assertEqual(primaries[0]["转换节点"], "flink-window-agg")

    def test_other_topic_only_maps_its_own_dead_letters(self) -> None:
        result = self.service.generate(topic_code="TOPI-0002")
        self.assertTrue(result["ok"])
        self.assertEqual(result["新增"], 1)
        mappings, _ = self.service.list_mappings()
        self.assertEqual({m["原始主题"] for m in mappings}, {"TOPI-0002"})
        self.assertEqual({m["死信编号"] for m in mappings}, {"DEAD-0002"})
        self.assertEqual({e["from"] for e in self.replay_edges()},
                         {"replay-partition:TOPI-0002:1"})


class TestReplayAtomicityAndResume(ReplayTestBase):
    def test_interruption_keeps_confirmed_pairs(self) -> None:
        result = self.service.generate(route=ROUTE_FULL, interrupt_after=2)
        self.assertTrue(result["中断"])
        self.assertEqual(result["新增"], 2)
        check = self.service.check()
        self.assertEqual(check["待补齐"], 1)
        self.assertTrue(check["atomic_ok"])
        mappings = store.rows(MAPPING_TABLE)
        edges = store.rows(EDGE_TABLE)
        self.assertEqual(len(mappings), 2)
        self.assertEqual(len(edges), 2)
        self.assertEqual({m["死信编号"] for m in mappings}, {"DEAD-0001"})

    def test_resume_only_fills_missing_items(self) -> None:
        generate = self.service.generate(route=ROUTE_FULL, interrupt_after=2)
        committed = {(m["映射编号"], m["确认时间"]) for m in store.rows(MAPPING_TABLE)}
        resumed = self.service.resume(interrupt_after=0)
        self.assertTrue(resumed["ok"])
        self.assertTrue(resumed["中断"])
        self.assertEqual(resumed["保留已确认"], 2)
        self.assertEqual(resumed["本次补齐"], 0)
        self.assertEqual({(m["映射编号"], m["确认时间"]) for m in store.rows(MAPPING_TABLE)}, committed)

        done = self.service.resume()
        self.assertFalse(done["中断"])
        self.assertEqual(done["保留已确认"], 2)
        self.assertEqual(done["本次补齐"], 1)
        check = self.service.check(int(generate["批次"]["id"]))
        self.assertTrue(check["all_confirmed"])
        self.assertTrue(check["atomic_ok"])
        self.assertEqual(self.service.check()["primary_ok"], True)

        again = self.service.resume()
        self.assertFalse(again["ok"])
        self.assertIn("没有可恢复", again["message"])

    def test_mapping_and_edge_written_together(self) -> None:
        self.service.generate(route=ROUTE_FULL)
        for mapping in store.rows(MAPPING_TABLE):
            paired = [
                edge for edge in store.rows(EDGE_TABLE)
                if edge["死信编号"] == mapping["死信编号"]
                and edge["转换节点"] == mapping["转换节点"]
                and edge["版本"] == mapping["版本"]
            ]
            self.assertEqual(len(paired), 1)
            self.assertEqual(paired[0]["from"], f"replay-partition:{mapping['原始主题']}:{mapping['原始分区']}")


class TestReplaySharedEntry(ReplayTestBase):
    def test_mark_replay_creates_traceable_mapping(self) -> None:
        entry, message = self.dead_letter.run_action(1, "标记重放")
        self.assertEqual(entry["status"], "已重放")
        self.assertEqual(entry["重放转换节点"], "flink-window-agg")
        self.assertEqual(entry["重放下游表名"], "order_agg_hourly")
        self.assertIn("主重放映射", message)
        self.assertTrue(self.replay_edges())

    def test_mark_replay_does_not_duplicate_primary(self) -> None:
        self.service.generate(route=ROUTE_FULL)
        before = len(store.rows(MAPPING_TABLE))
        entry, _ = self.dead_letter.run_action(1, "标记重放")
        self.assertEqual(len(store.rows(MAPPING_TABLE)), before)
        self.assertEqual(entry["重放映射状态"], "已重放")

    def test_lineage_and_dead_letter_share_same_data(self) -> None:
        self.service.generate(route=ROUTE_FULL)
        shared = self.lineage.replay_mappings()
        direct, _ = self.service.list_mappings()
        self.assertEqual(shared["total"], len(direct))
        self.assertEqual(shared["version"], self.service.latest_version())
        self.assertEqual(len(shared["items"]), 3)

    def test_import_uses_same_rules(self) -> None:
        csv_text = (
            "死信编号,原始分区,转换节点,主重放\n"
            "DEAD-0001,0,flink-window-agg,是\n"
            "DEAD-0001,0,flink-order-risk,否\n"
            "DEAD-0001,9,flink-window-agg,否\n"
            "DEAD-0002,1,flink-window-agg,否\n"
        )
        result = self.service.import_csv(csv_text)
        self.assertTrue(result["ok"])
        self.assertEqual(result["新增"], 2)
        self.assertEqual(len(result["跳过"]), 2)
        mappings, _ = self.service.list_mappings()
        self.assertEqual(len([m for m in mappings if m["主重放"]]), 1)
        self.assertTrue(self.service.check()["atomic_ok"])

    def test_package_contains_mapping_and_edges(self) -> None:
        self.service.generate(route=ROUTE_FULL)
        data = self.service.package_bytes()
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            names = archive.namelist()
            self.assertIn("dead_letter_replay.csv", names)
            self.assertIn("stream_lineage_replay_edges.csv", names)
            rows = archive.read("dead_letter_replay.csv").decode("utf-8-sig").splitlines()
            self.assertGreaterEqual(len(rows), 4)


if __name__ == "__main__":
    unittest.main()
