"""补充规则：权限、设备阻塞、后继链、非冲突并发、交接清单序列化。"""

from __future__ import annotations

import json
import sys
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tournament_turnaround import DomainError, TurnaroundHub, build_handover  # noqa: E402
from tournament_turnaround.state import PROPOSED, WAITING  # noqa: E402

CST = timezone(timedelta(hours=8))
DAY = datetime(2026, 9, 27, 8, 0, tzinfo=CST)


def at(hour: int, minute: int = 0) -> datetime:
    return DAY.replace(hour=hour, minute=minute)


class GuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.hub = TurnaroundHub()
        self.hub.register_tournament("WTA250", "宁波女子公开赛", ["wta-officer-1"])
        self.hub.register_tournament("ATP-CHQ", "男子挑战赛资格赛", ["atp-officer-1"])
        self.hub.register_resource("court-1", "court", "中心场", "WTA 配置")
        self.hub.register_resource("court-2", "court", "2 号场", "训练配置")

    def test_only_duty_officer_confirms(self) -> None:
        prop = self.hub.submit_proposal(
            "SHIFT", "WTA250", "court-1", at(10), at(11), "人员班次", confirm_by=at(9))
        with self.assertRaises(DomainError):
            self.hub.confirm_proposal(prop, "atp-officer-1", now=at(8))
        # 其他赛事值班长的驳回尝试也被拒绝
        with self.assertRaises(DomainError):
            self.hub.reject_proposal(prop, "atp-officer-1", "越权驳回")

    def test_equipment_down_blocks_confirmation(self) -> None:
        self.hub.equipment_down("court-1", "空调分区A")
        prop = self.hub.submit_proposal(
            "MAINTENANCE", "WTA250", "court-1", at(10), at(12), "屋盖维护",
            confirm_by=at(9), requires_equipment=["空调分区A"])
        with self.assertRaises(DomainError):
            self.hub.confirm_proposal(prop, "wta-officer-1", now=at(8))
        self.assertEqual(PROPOSED, self.hub.state.proposals[prop].status)

    def test_non_conflicting_concurrent_confirms_both_succeed(self) -> None:
        """同一资源不同时段：资源流串行化带来的版本冲突应自动重试，两方都成功。"""
        morning = self.hub.submit_proposal(
            "MATCH", "WTA250", "court-1", at(10), at(12), "早场", confirm_by=at(9))
        evening = self.hub.submit_proposal(
            "MATCH", "WTA250", "court-1", at(14), at(16), "晚场", confirm_by=at(13))
        outcomes: list[str] = []

        def confirm(prop: str) -> None:
            try:
                self.hub.confirm_proposal(prop, "wta-officer-1", now=at(8))
                outcomes.append("ok")
            except DomainError:
                outcomes.append("fail")

        threads = [threading.Thread(target=confirm, args=(morning,)),
                   threading.Thread(target=confirm, args=(evening,))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(["ok", "ok"], sorted(outcomes))
        self.assertEqual(2, len(self.hub.state.occupying("court-1")))

    def test_disruption_moves_whole_unstarted_chain(self) -> None:
        """后继链中两场未开赛比赛都要顺延，已完赛的前序不动。"""
        self.hub.set_match("m1", "ATP-CHQ", "QUALIFYING", "QR1", resource_id="court-1")
        self.hub.set_match("m2", "ATP-CHQ", "QUALIFYING", "QR2", predecessors=["m1"], resource_id="court-1")
        self.hub.set_match("m3", "ATP-CHQ", "MAIN", "R1", predecessors=["m2"], resource_id="court-1")

        def schedule(match_id: str, start: datetime, end: datetime) -> str:
            prop = self.hub.submit_proposal(
                "MATCH", "ATP-CHQ", "court-1", start, end, match_id,
                confirm_by=start - timedelta(hours=1), match_id=match_id)
            self.hub.confirm_proposal(prop, "atp-officer-1", now=start - timedelta(hours=2))
            return prop

        p1 = schedule("m1", at(9), at(10))
        self.hub.start_match("m1", at=at(9))
        self.hub.complete_match("m1", "q-player", "6-0 6-0", at=at(10))
        self.hub.release_resource(p1, at=at(10, 5))
        schedule("m2", at(10, 30), at(12))
        schedule("m3", at(12), at(13))

        new_ids = self.hub.revise_for_disruption(
            "m2", "RAIN", at(14), at=at(11), confirm_by=at(15))
        self.assertEqual(2, len(new_ids))  # m2 与 m3 都重排，m1 已完赛不动
        self.assertEqual("COMPLETED", self.hub.state.matches["m1"].status)
        self.assertEqual("NOT_STARTED", self.hub.state.matches["m2"].status)
        self.assertEqual(new_ids[0], self.hub.state.matches["m2"].scheduled_proposal)
        self.assertEqual(new_ids[1], self.hub.state.matches["m3"].scheduled_proposal)
        # 新承诺按顺序错开，互不重叠
        starts = [self.hub.state.proposals[i].start for i in new_ids]
        self.assertTrue(starts[1] >= self.hub.state.proposals[new_ids[0]].end)

    def test_cross_tournament_dependency_rejected(self) -> None:
        self.hub.set_match("w1", "WTA250", "MAIN", "F")
        with self.assertRaises(DomainError):
            self.hub.set_match("a1", "ATP-CHQ", "QUALIFYING", "QR1", predecessors=["w1"])

    def test_handover_is_json_serializable(self) -> None:
        self.hub.record_config_transition("court-1", "WTA 配置", "ATP 配置", at(18))
        self.hub.complete_inspection("court-1", "WTA 配置", "ATP 配置", "王主管", at(18, 30))
        self.hub.submit_proposal(
            "SECURITY", "ATP-CHQ", "court-1", at(21), at(22), "安保分区切换", confirm_by=at(20))
        handover = build_handover(self.hub.state, at(19))
        raw = json.dumps(handover.to_dict(), ensure_ascii=False)
        decoded = json.loads(raw)
        self.assertEqual("王主管", decoded["config_timeline"][0]["inspected_by"])
        self.assertEqual("SECURITY", decoded["pending_commitments"][0]["category"])
        self.assertIn("安保分区切换", raw)


if __name__ == "__main__":
    unittest.main()
