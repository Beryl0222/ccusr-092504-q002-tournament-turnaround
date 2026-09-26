"""端到端场景：宁波网球中心女子公开赛收官日衔接男子挑战赛资格赛。"""

from __future__ import annotations

import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tournament_turnaround import (  # noqa: E402
    DomainError,
    JsonlEventStore,
    ProposalWaitlisted,
    TurnaroundHub,
    build_handover,
)
from tournament_turnaround.state import CONFIRMED, PROPOSED, SUPERSEDED, WAITING  # noqa: E402

CST = timezone(timedelta(hours=8))
DAY = datetime(2026, 9, 27, 8, 0, tzinfo=CST)  # 女子决赛日 / 男子资格赛首日


def at(hour: int, minute: int = 0) -> datetime:
    return DAY.replace(hour=hour, minute=minute)


class HubFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.hub = TurnaroundHub()
        self.hub.register_tournament("WTA250", "宁波女子公开赛", ["wta-officer-1", "wta-officer-2"])
        self.hub.register_tournament("ATP-CHQ", "男子挑战赛资格赛", ["atp-officer-1"])
        self.hub.register_resource("court-1", "court", "中心场", "WTA 决赛配置")
        self.hub.register_resource("court-2", "court", "2 号场", "训练配置")


class ConcurrencyTests(HubFixture):
    def test_concurrent_confirmation_only_one_wins(self) -> None:
        """两个赛事值班长同时确认同一时段：一方成功，另一方进候补。"""
        wta_prop = self.hub.submit_proposal(
            "MATCH", "WTA250", "court-1", at(14), at(16), "女单决赛",
            confirm_by=at(13), priority=10, match_id=None,
        )
        atp_prop = self.hub.submit_proposal(
            "TRAINING", "ATP-CHQ", "court-1", at(14, 30), at(16, 30), "资格赛球员热身",
            confirm_by=at(13), priority=50,
        )
        # 第二个提交时第一个还只是建议，不互斥；值班长确认时才裁决
        results: dict[str, str] = {}

        def confirm(prop: str, officer: str, key: str) -> None:
            try:
                self.hub.confirm_proposal(prop, officer, now=at(12))
                results[key] = "confirmed"
            except ProposalWaitlisted:
                results[key] = "waitlisted"

        threads = [
            threading.Thread(target=confirm, args=(wta_prop, "wta-officer-1", "wta")),
            threading.Thread(target=confirm, args=(atp_prop, "atp-officer-1", "atp")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(sorted(results.values()), ["confirmed", "waitlisted"])
        confirmed_id = wta_prop if results["wta"] == "confirmed" else atp_prop
        waiting_id = atp_prop if results["wta"] == "confirmed" else wta_prop
        self.assertEqual(CONFIRMED, self.hub.state.proposals[confirmed_id].status)
        self.assertEqual(WAITING, self.hub.state.proposals[waiting_id].status)
        # 同一时段只有一方占用
        occupying = self.hub.state.occupying("court-1")
        self.assertEqual(1, len(occupying))

    def test_release_advances_waitlist_in_explainable_order(self) -> None:
        """释放后按（优先级、到达先后）推进，顺序可解释。"""
        first = self.hub.submit_proposal(
            "MATCH", "WTA250", "court-1", at(14), at(16), "女双决赛", confirm_by=at(13), priority=10)
        self.hub.confirm_proposal(first, "wta-officer-1", now=at(12))

        low = self.hub.submit_proposal(
            "TRAINING", "ATP-CHQ", "court-1", at(14), at(15), "低优先级热身", confirm_by=at(13, 30), priority=90)
        high = self.hub.submit_proposal(
            "MATCH", "ATP-CHQ", "court-1", at(14), at(15, 30), "资格赛首轮", confirm_by=at(13, 30), priority=20)
        self.assertEqual(WAITING, self.hub.state.proposals[low].status)
        self.assertEqual(WAITING, self.hub.state.proposals[high].status)

        self.hub.release_resource(first, at=at(16))
        # 高优先级（数字小）先被推进为待确认，低优先级仍候补
        self.assertEqual(PROPOSED, self.hub.state.proposals[high].status)
        self.assertEqual(WAITING, self.hub.state.proposals[low].status)

        handover = build_handover(self.hub.state, at(16))
        queue = [w for w in handover.waitlists if w.resource_id == "court-1"]
        self.assertEqual(1, len(queue))
        self.assertEqual([low], [item["proposal_id"] for item in queue[0].order])
        self.assertIn("优先级 90", queue[0].order[0]["explanation"])


class RosterRuleTests(HubFixture):
    def _seed_draw(self) -> None:
        self.hub.confirm_entry("ATP-CHQ", "p-direct", "直接入围者", 80, "DIRECT")
        self.hub.confirm_entry("ATP-CHQ", "p-alt-1", "候补甲", 120, "ALTERNATE")
        self.hub.confirm_entry("ATP-CHQ", "p-alt-2", "候补乙", 150, "ALTERNATE")
        self.hub.open_stage("ATP-CHQ", "MAIN")
        self.hub.fill_slot("ATP-CHQ", "slot-1", "MAIN", "R1", "DIRECT", "p-direct")

    def test_alternate_promoted_by_rank_before_lock(self) -> None:
        self._seed_draw()
        self.hub.withdraw_player("ATP-CHQ", "p-direct")
        slot = self.hub.state._slot("ATP-CHQ", "slot-1")
        self.assertEqual("p-alt-1", slot.player_id)  # 排名更高者优先
        entry = [e for e in self.hub.state.entries["ATP-CHQ"] if e.player_id == "p-alt-1"][0]
        self.assertEqual("DIRECT", entry.stage)

    def test_lucky_loser_after_lock(self) -> None:
        self._seed_draw()
        self.hub.lock_stage("ATP-CHQ", "MAIN")
        self.hub.withdraw_player("ATP-CHQ", "p-direct")
        slot = self.hub.state._slot("ATP-CHQ", "slot-1")
        self.assertEqual("p-alt-1", slot.player_id)
        self.assertEqual("LUCKY_LOSER", slot.source)

    def test_qualifier_slot_never_filled_manually(self) -> None:
        self.hub.confirm_entry("ATP-CHQ", "p-q", "资格赛球员", 200, "QUALIFYING")
        self.hub.open_stage("ATP-CHQ", "MAIN")
        self.hub.register_qualifier_slot("ATP-CHQ", "slot-q1", "MAIN", "R1")
        with self.assertRaises(DomainError):
            self.hub.fill_slot("ATP-CHQ", "slot-q1", "MAIN", "R1", "DIRECT", "p-q")

    def test_qualifier_slot_filled_only_by_qualifying_result(self) -> None:
        self.hub.confirm_entry("ATP-CHQ", "p-q", "资格赛球员", 200, "QUALIFYING")
        self.hub.open_stage("ATP-CHQ", "MAIN")
        self.hub.register_qualifier_slot("ATP-CHQ", "slot-q1", "MAIN", "R1")
        self.hub.set_match("q-match-1", "ATP-CHQ", "QUALIFYING", "QR2")
        self.hub.start_match("q-match-1", at=at(10))
        self.hub.complete_match("q-match-1", "p-q", "6-4 6-4", at=at(12))
        slot = self.hub.state._slot("ATP-CHQ", "slot-q1")
        self.assertEqual("p-q", slot.player_id)
        self.assertEqual("q-match-1", slot.via_match)

    def test_no_double_booking_of_player(self) -> None:
        self._seed_draw()
        with self.assertRaises(DomainError):
            self.hub.fill_slot("ATP-CHQ", "slot-2", "MAIN", "R1", "DIRECT", "p-direct")


class RevisionImmutabilityTests(HubFixture):
    def _schedule_match(self, match_id: str, start_h: int, end_h: int,
                        predecessors: list[str] | None = None) -> str:
        self.hub.set_match(match_id, "WTA250", "MAIN", "F", predecessors=predecessors)
        prop = self.hub.submit_proposal(
            "MATCH", "WTA250", "court-1", at(start_h), at(end_h), f"比赛 {match_id}",
            confirm_by=at(start_h - 1), priority=10, match_id=match_id)
        self.hub.confirm_proposal(prop, "wta-officer-1", now=at(start_h - 2))
        return prop

    def test_completed_result_cannot_be_rewritten(self) -> None:
        prop = self._schedule_match("wta-final", 14, 16)
        self.hub.start_match("wta-final", at=at(14))
        self.hub.complete_match("wta-final", "player-a", "7-5 6-3", at=at(16))
        with self.assertRaises(DomainError):
            self.hub.complete_match("wta-final", "player-b", "6-0 6-0", at=at(17))
        with self.assertRaises(DomainError):
            self.hub.revise_for_disruption("wta-final", "RAIN", at(18))
        self.assertEqual("player-a", self.hub.state.matches["wta-final"].winner_id)

    def test_rain_revision_moves_only_not_started_chain(self) -> None:
        semi = self._schedule_match("wta-semi", 12, 14)
        self.hub.start_match("wta-semi", at=at(12))
        self.hub.complete_match("wta-semi", "player-a", "6-2 6-2", at=at(14))
        self.hub.release_resource(semi, at=at(14, 10))  # 完赛后场地交还
        final_prop = self._schedule_match("wta-final", 16, 18, predecessors=["wta-semi"])

        self.hub.record_player_movement("player-a", "球员甲", "更衣室", "球员通道", at=at(15, 30))
        movements_before = list(self.hub.state.player_streams["player-a"])

        new_ids = self.hub.revise_for_disruption(
            "wta-final", "RAIN", at(19), at=at(18, 30), confirm_by=at(20))
        self.assertEqual(1, len(new_ids))
        # 旧承诺被取代并释放，新承诺仍需值班长确认
        self.assertEqual(SUPERSEDED, self.hub.state.proposals[final_prop].status)
        self.assertEqual(PROPOSED, self.hub.state.proposals[new_ids[0]].status)
        self.assertEqual([], self.hub.state.occupying("court-1"))
        # 完赛成绩与动线保持原样
        self.assertEqual("player-a", self.hub.state.matches["wta-semi"].winner_id)
        self.assertEqual(movements_before, self.hub.state.player_streams["player-a"])
        # 新承诺确认后才重新占用
        self.hub.confirm_proposal(new_ids[0], "wta-officer-2", now=at(19))
        self.assertEqual(1, len(self.hub.state.occupying("court-1")))

    def test_revision_of_started_match_rejected(self) -> None:
        self._schedule_match("wta-final", 14, 16)
        self.hub.start_match("wta-final", at=at(14))
        with self.assertRaises(DomainError):
            self.hub.revise_for_disruption("wta-final", "OVERRUN", at(17))


class RestartDeadlineTests(unittest.TestCase):
    def test_restart_keeps_original_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            hub = TurnaroundHub(JsonlEventStore(path))
            hub.register_tournament("ATP-CHQ", "男子挑战赛资格赛", ["atp-officer-1"])
            hub.register_resource("court-2", "court", "2 号场", "训练配置")
            prop = hub.submit_proposal(
                "TRAINING", "ATP-CHQ", "court-2", at(9), at(10), "早场训练",
                confirm_by=at(8, 30), priority=50)

            # 服务重启：从事件日志重建，截止时间仍是原来的 08:30
            restarted = TurnaroundHub(JsonlEventStore(path))
            self.assertEqual(at(8, 30), restarted.state.proposals[prop].confirm_by)
            with self.assertRaises(DomainError):
                restarted.confirm_proposal(prop, "atp-officer-1", now=at(9))
            self.assertEqual("EXPIRED", restarted.state.proposals[prop].status)

            # 再次重启后失效状态也在
            again = TurnaroundHub(JsonlEventStore(path))
            self.assertEqual("EXPIRED", again.state.proposals[prop].status)

    def test_expire_due_sweeps_overdue(self) -> None:
        hub = TurnaroundHub()
        hub.register_tournament("ATP-CHQ", "男子挑战赛资格赛", ["atp-officer-1"])
        hub.register_resource("court-2", "court", "2 号场", "训练配置")
        fresh = hub.submit_proposal(
            "TRAINING", "ATP-CHQ", "court-2", at(20), at(21), "晚场训练", confirm_by=at(19))
        stale = hub.submit_proposal(
            "TRAINING", "ATP-CHQ", "court-2", at(10), at(11), "早场训练", confirm_by=at(9))
        expired = hub.expire_due(now=at(12))
        self.assertEqual([stale], expired)
        self.assertEqual(PROPOSED, hub.state.proposals[fresh].status)


class HandoverTests(HubFixture):
    def test_handover_lists_transitions_inspections_and_blockers(self) -> None:
        # 场地配置转换：WTA 决赛配置 → ATP 资格赛配置
        self.hub.record_config_transition("court-1", "WTA 决赛配置", "ATP 资格赛配置", at(18))
        self.hub.complete_inspection("court-1", "WTA 决赛配置", "ATP 资格赛配置", "场地主管-王", at(18, 45))
        self.hub.record_config_transition("court-1", "ATP 资格赛配置", "ATP 正赛配置", at(22))

        # 设备停用阻塞比赛
        self.hub.equipment_down("court-1", "电子司线")
        self.hub.set_match("atp-q1", "ATP-CHQ", "QUALIFYING", "QR1",
                           requires_equipment=["电子司线"], resource_id="court-1")
        self.hub.set_match("atp-q2", "ATP-CHQ", "QUALIFYING", "QR2",
                           predecessors=["atp-q1"], resource_id="court-1")

        handover = build_handover(self.hub.state, at(19))
        timeline = handover.config_timeline
        self.assertEqual(2, len(timeline))
        self.assertEqual("场地主管-王", timeline[0].inspected_by)
        self.assertIsNone(timeline[1].inspected_by)

        blocked = {row.match_id: row for row in handover.blocked_matches}
        self.assertIn("atp-q1", blocked)
        self.assertTrue(any("电子司线" in b for b in blocked["atp-q1"].blockers))
        self.assertTrue(any("atp-q1" in b for b in blocked["atp-q2"].blockers))

        # 设备恢复后阻塞解除（赛程承诺仍未确认，所以还有别的阻塞项）
        self.hub.equipment_restored("court-1", "电子司线")
        handover2 = build_handover(self.hub.state, at(19))
        blocked2 = {row.match_id: row for row in handover2.blocked_matches}
        self.assertFalse(any("电子司线" in b for b in blocked2["atp-q1"].blockers))


if __name__ == "__main__":
    unittest.main()
