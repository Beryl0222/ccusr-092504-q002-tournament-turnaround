"""并发确认与候补顺序：两个团队同时确认同一时段只有一方成功。"""

from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tournament_turnaround.handover import build_handover, render_text
from tournament_turnaround.hub import (
    AuthorizationError,
    ConflictError,
    NotFoundError,
    RuleViolation,
    TurnaroundHub,
)
from tournament_turnaround.model import (
    CommitmentState,
    DrawStage,
    EntryStatus,
    MatchState,
    ResourceKind,
    TimeWindow,
    VenueConfig,
)
from tournament_turnaround.store import ConcurrentAppendError, JsonlEventStore

TZ = timezone(timedelta(hours=8))


def t(day: int = 27, hour: int = 10, minute: int = 0) -> datetime:
    return datetime(2026, 9, day, hour, minute, tzinfo=TZ)


def window(start: datetime, end: datetime) -> TimeWindow:
    return TimeWindow(start, end)


def make_hub(path: str, now: datetime | None = None) -> TurnaroundHub:
    current = [now or t(27, 8, 0)]
    store = JsonlEventStore(path)
    hub = TurnaroundHub(store, clock=lambda: current[0])
    hub.restore()
    hub._clock_state = current  # type: ignore[attr-defined]
    return hub


def advance(hub: TurnaroundHub, now: datetime) -> None:
    hub._clock_state[0] = now  # type: ignore[attr-defined]


def seed_basic(hub: TurnaroundHub) -> None:
    hub.register_tournament("WTO", "宁波女子公开赛", ["zhang"], priority=1)
    hub.register_tournament("MCH", "男子挑战赛", ["li"], priority=2)
    hub.register_resource("C1", ResourceKind.COURT, "中心球场")
    hub.register_resource("C2", ResourceKind.COURT, "二号球场")
    hub.register_resource("ROOF", ResourceKind.EQUIPMENT, "可开启屋盖")


class ConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False)
        self.tmp.close()
        self.hub = make_hub(self.tmp.name)
        seed_basic(self.hub)

    def test_two_concurrent_confirms_only_one_succeeds(self) -> None:
        w = window(t(27, 10), t(27, 12))
        women = self.hub.schedule_match("w-final", "WTO", "女单末轮", "C1", w,
                                        proposed_by="scheduler", deadline=t(27, 9, 30))
        men = self.hub.propose_commitment(
            "MCH", ResourceKind.COURT, "C1", window(t(27, 11), t(27, 13)),
            purpose="男挑资格 Q1", proposed_by="scheduler2", deadline=t(27, 9, 30),
        )
        # 女网值班长先确认成功
        self.hub.confirm_commitment(women.commitment_id, "zhang")
        self.assertEqual(self.hub.commitments[women.commitment_id].state, CommitmentState.CONFIRMED)
        # 男挑值班长同时段确认失败：建议进入候补而非占用
        with self.assertRaises(ConflictError):
            self.hub.confirm_commitment(men.commitment_id, "li")
        self.assertEqual(self.hub.commitments[men.commitment_id].state, CommitmentState.WAITING)

    def test_only_registered_officer_may_confirm(self) -> None:
        c = self.hub.propose_commitment(
            "WTO", ResourceKind.COURT, "C1", window(t(27, 14), t(27, 15)),
            purpose="训练", proposed_by="scheduler",
        )
        with self.assertRaises(AuthorizationError):
            self.hub.confirm_commitment(c.commitment_id, "stranger")

    def test_double_confirm_same_commitment_is_idempotent_rejection(self) -> None:
        c = self.hub.propose_commitment(
            "WTO", ResourceKind.COURT, "C2", window(t(27, 14), t(27, 15)),
            purpose="训练", proposed_by="scheduler",
        )
        self.hub.confirm_commitment(c.commitment_id, "zhang")
        with self.assertRaises(ConflictError):
            self.hub.confirm_commitment(c.commitment_id, "zhang")

    def test_store_level_cas_rejects_stale_writer(self) -> None:
        store = self.hub.store
        event = {
            "event_id": "stale",
            "event_type": "DEADLINE_SET",
            "aggregate_type": "turnaround_plan",
            "aggregate_id": "turnaround-plan",
            "occurred_at": t(27, 8).isoformat(),
            "version": 1,
            "summary": "陈旧写入",
        }
        with self.assertRaises(ConcurrentAppendError):
            store.append(event, expected_version=999)

    def test_parallel_confirms_across_threads_only_one_wins(self) -> None:
        import threading

        results: dict[str, str] = {}

        def confirm(cid: str, officer: str, tag: str) -> None:
            try:
                self.hub.confirm_commitment(cid, officer)
                results[tag] = "won"
            except ConflictError:
                results[tag] = "lost"

        c_a = self.hub.propose_commitment(
            "WTO", ResourceKind.COURT, "C1", window(t(27, 16), t(27, 18)),
            purpose="女网补练", proposed_by="s1",
        )
        c_b = self.hub.propose_commitment(
            "MCH", ResourceKind.COURT, "C1", window(t(27, 17), t(27, 19)),
            purpose="男挑加练", proposed_by="s2",
        )
        threads = [
            threading.Thread(target=confirm, args=(c_a.commitment_id, "zhang", "A")),
            threading.Thread(target=confirm, args=(c_b.commitment_id, "li", "B")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(results.values()), ["lost", "won"])
        confirmed = [c for c in self.hub.commitments.values() if c.state == CommitmentState.CONFIRMED]
        waiting = [c for c in self.hub.commitments.values() if c.state == CommitmentState.WAITING]
        self.assertEqual(len(confirmed), 1)
        self.assertEqual(len(waiting), 1)


class WaitlistTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False)
        self.tmp.close()
        self.hub = make_hub(self.tmp.name)
        seed_basic(self.hub)

    def test_waitlist_order_is_explainable_and_advances_after_release(self) -> None:
        holder = self.hub.propose_commitment(
            "WTO", ResourceKind.COURT, "C1", window(t(27, 10), t(27, 14)),
            purpose="女网占用", proposed_by="scheduler",
        )
        self.hub.confirm_commitment(holder.commitment_id, "zhang")

        # 两个候补：窗口更早者排序在前；午场与早场重叠，早场放行后午场仍须等待
        mid = self.hub.propose_commitment(
            "MCH", ResourceKind.COURT, "C1", window(t(27, 11, 30), t(27, 12, 30)),
            purpose="男挑午场", proposed_by="scheduler2",
        )
        early = self.hub.propose_commitment(
            "MCH", ResourceKind.COURT, "C1", window(t(27, 11), t(27, 12)),
            purpose="男挑早场", proposed_by="scheduler2",
        )
        with self.assertRaises(ConflictError):
            self.hub.confirm_commitment(mid.commitment_id, "li")
        with self.assertRaises(ConflictError):
            self.hub.confirm_commitment(early.commitment_id, "li")

        queue = self.hub.waiting_commitments(ResourceKind.COURT, "C1")
        self.assertEqual([c.purpose for c in queue], ["男挑早场", "男挑午场"])
        self.assertIsNotNone(queue[0].waitlist_seq)

        # 释放后：早场先推进；午场与其重叠，继续候补
        self.hub.release_commitment(holder.commitment_id, "zhang", "女网结束")
        queue = self.hub.waiting_commitments(ResourceKind.COURT, "C1")
        self.assertEqual([c.purpose for c in queue], ["男挑午场"])
        advanced = self.hub.commitments[early.commitment_id]
        self.assertEqual(advanced.state, CommitmentState.PROPOSED)
        self.hub.confirm_commitment(early.commitment_id, "li")
        self.assertEqual(self.hub.commitments[early.commitment_id].state, CommitmentState.CONFIRMED)
        self.assertEqual(self.hub.commitments[mid.commitment_id].state, CommitmentState.WAITING)

    def test_adjacent_windows_do_not_conflict(self) -> None:
        first = self.hub.propose_commitment(
            "WTO", ResourceKind.COURT, "C2", window(t(27, 10), t(27, 12)),
            purpose="前一场", proposed_by="scheduler",
        )
        second = self.hub.propose_commitment(
            "MCH", ResourceKind.COURT, "C2", window(t(27, 12), t(27, 14)),
            purpose="无缝衔接场", proposed_by="scheduler2",
        )
        self.hub.confirm_commitment(first.commitment_id, "zhang")
        self.hub.confirm_commitment(second.commitment_id, "li")
        self.assertEqual(self.hub.commitments[second.commitment_id].state, CommitmentState.CONFIRMED)


class DeadlineRestartTests(unittest.TestCase):
    def test_pending_proposal_expires_at_original_deadline_after_restart(self) -> None:
        path = tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False).name
        hub = make_hub(path, now=t(27, 8))
        seed_basic(hub)
        proposal = hub.propose_commitment(
            "WTO", ResourceKind.COURT, "C1", window(t(27, 15), t(27, 16)),
            purpose="末轮补赛", proposed_by="scheduler", deadline=t(27, 9, 0),
        )
        cid = proposal.commitment_id
        self.assertEqual(hub.commitments[cid].state, CommitmentState.PROPOSED)

        # 服务重启，时钟已超过原截止时间
        restarted = make_hub(path, now=t(27, 10))
        self.assertEqual(restored_state := restarted.commitments[cid].state, CommitmentState.RELEASED)
        self.assertIn("超过确认截止", restarted.commitments[cid].reject_reason or "")

        # 事件流只追加：历史事件仍然全部可查
        self.assertGreaterEqual(len(restarted.store.load()), 4)

    def test_waiting_proposal_keeps_deadline_and_expires_while_waiting(self) -> None:
        path = tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False).name
        hub = make_hub(path, now=t(27, 8))
        seed_basic(hub)
        holder = hub.propose_commitment(
            "WTO", ResourceKind.COURT, "C1", window(t(27, 10), t(27, 18)),
            purpose="全天占用", proposed_by="scheduler",
        )
        hub.confirm_commitment(holder.commitment_id, "zhang")
        waiter = hub.propose_commitment(
            "MCH", ResourceKind.COURT, "C1", window(t(27, 11), t(27, 12)),
            purpose="过期候补", proposed_by="scheduler2", deadline=t(27, 9),
        )
        with self.assertRaises(ConflictError):
            hub.confirm_commitment(waiter.commitment_id, "li")
        self.assertEqual(hub.commitments[waiter.commitment_id].state, CommitmentState.WAITING)

        restarted = make_hub(path, now=t(27, 10))
        self.assertEqual(restarted.commitments[waiter.commitment_id].state, CommitmentState.RELEASED)
        # 占用方不受影响
        self.assertEqual(restarted.commitments[holder.commitment_id].state, CommitmentState.CONFIRMED)


class EntryReplacementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False)
        self.tmp.close()
        self.hub = make_hub(self.tmp.name)
        seed_basic(self.hub)

    def _seed_entries(self) -> tuple[str, str, str]:
        hub = self.hub
        hub.confirm_entry("e-a", "MCH", "player-A", stage=DrawStage.QUALIFYING, occupies_slot="Q1")
        hub.confirm_entry("e-b", "MCH", "player-B", stage=DrawStage.QUALIFYING, occupies_slot="Q2")
        hub.confirm_entry("e-alt1", "MCH", "player-R1", stage=DrawStage.QUALIFYING)
        hub.confirm_entry("e-alt2", "MCH", "player-R2", stage=DrawStage.QUALIFYING)
        hub.confirm_entry("e-main-alt", "MCH", "player-MR", stage=DrawStage.MAIN_DRAW)
        hub.set_draw_stage("MCH", DrawStage.QUALIFYING)
        return "e-a", "e-alt1", "e-alt2"

    def test_withdrawal_auto_replaces_with_stage_alternate(self) -> None:
        withdrawn, alt1, _ = self._seed_entries()
        replaced_id = self.hub.withdraw(withdrawn)
        self.assertEqual(replaced_id, alt1)
        replacement = self.hub.entries[alt1]
        self.assertEqual(replacement.status, EntryStatus.REPLACEMENT)
        self.assertEqual(replacement.occupies_slot, "Q1")
        self.assertEqual(replacement.replaced_entry_id, withdrawn)

    def test_qualifying_alternate_cannot_jump_to_main_draw_slot(self) -> None:
        hub = self.hub
        hub.confirm_entry("m1", "MCH", "main-p1", stage=DrawStage.MAIN_DRAW, occupies_slot="M1")
        hub.confirm_entry("q-alt", "MCH", "q-alt-player", stage=DrawStage.QUALIFYING)
        hub.set_draw_stage("MCH", DrawStage.MAIN_DRAW)
        hub.withdraw("m1")  # 正赛阶段退赛
        # 资格赛候补不得借递补越过资格赛
        with self.assertRaises(RuleViolation):
            hub.replace_withdrawn("m1", "q-alt")

    def test_already_placed_replacement_cannot_double_occupy(self) -> None:
        withdrawn, alt1, alt2 = self._seed_entries()
        self.hub.withdraw(withdrawn)  # alt1 已接管 Q1
        self.hub.confirm_entry("e-c", "MCH", "player-C", stage=DrawStage.QUALIFYING, occupies_slot="Q3")
        # 退赛自动递补：alt2 接管 Q3
        self.assertEqual(self.hub.withdraw("e-c"), alt2)
        self.assertEqual(self.hub.entries[alt2].occupies_slot, "Q3")
        # 再出现空缺席位时，已占位的 alt1（REPLACEMENT）不能再次递补
        self.hub.confirm_entry("e-d", "MCH", "player-D", stage=DrawStage.QUALIFYING, occupies_slot="Q4")
        self.assertIsNone(self.hub.withdraw("e-d"))  # 已无候补
        with self.assertRaises(RuleViolation):
            self.hub.replace_withdrawn("e-d", alt1)
        self.assertEqual(self.hub.entries[alt1].occupies_slot, "Q1")

    def test_withdrawal_without_alternate_leaves_open_slot(self) -> None:
        self.hub.confirm_entry("solo", "MCH", "solo-player", stage=DrawStage.QUALIFYING, occupies_slot="Q9")
        self.hub.set_draw_stage("MCH", DrawStage.QUALIFYING)
        self.assertIsNone(self.hub.withdraw("solo"))
        handover = build_handover(self.hub, now=t(27, 9))
        self.assertTrue(any("Q9" in line for line in handover.open_withdrawals))


class RescheduleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False)
        self.tmp.close()
        self.hub = make_hub(self.tmp.name)
        seed_basic(self.hub)

    def test_completed_scores_and_live_movements_are_never_rewritten(self) -> None:
        hub = self.hub
        w1 = window(t(27, 10), t(27, 12))
        hub.schedule_match("m1", "WTO", "半决赛", "C1", w1, player_ids=["p1", "p2"])
        hub.confirm_commitment(
            [c for c in hub.commitments.values() if c.match_id == "m1"][0].commitment_id, "zhang"
        )
        hub.start_match("m1")
        hub.lock_player_movement("m1", "球员通道A→中心球场")
        advance(hub, t(27, 12, 30))
        hub.complete_match("m1", "6-4 7-5", "p1")

        # 试图把已结束比赛纳入重排 -> 拒绝
        with self.assertRaises(RuleViolation):
            hub.reschedule_due_to_disruption(
                ["m1"], {"m1": ("C2", window(t(27, 14), t(27, 16)))}, reason="降雨"
            )
        record = hub.matches["m1"]
        self.assertEqual(record.state, MatchState.COMPLETED)
        self.assertEqual(record.score, "6-4 7-5")
        self.assertEqual(record.winner_id, "p1")
        self.assertEqual(len(hub.movements["m1"]), 1)
        self.assertEqual(record.schedule_version, 1)

    def test_rain_reschedules_only_unstarted_dependency_chain(self) -> None:
        hub = self.hub
        hub.schedule_match("qf", "WTO", "八强", "C1", window(t(27, 10), t(27, 12)))
        hub.schedule_match(
            "sf", "WTO", "四强", "C1", window(t(27, 13), t(27, 15)),
            depends_on=["qf"],
        )
        advance(hub, t(27, 9, 30))
        # 降雨：八强与其下游四强一起重排到 C2 新时段
        new = hub.reschedule_due_to_disruption(
            ["qf"],
            {
                "qf": ("C2", window(t(27, 11), t(27, 13))),
                "sf": ("C2", window(t(27, 14), t(27, 16))),
            },
            reason="降雨",
        )
        self.assertEqual(len(new), 2)
        self.assertEqual(hub.matches["qf"].court_id, "C2")
        self.assertEqual(hub.matches["sf"].court_id, "C2")
        self.assertEqual(hub.matches["sf"].schedule_version, 2)
        # 依赖关系保留
        self.assertEqual(hub.matches["sf"].depends_on, ("qf",))
        # 旧承诺均已释放
        old = [c for c in hub.commitments.values() if c.purpose == "比赛 qf" and c.state == CommitmentState.RELEASED]
        self.assertTrue(old)

    def test_on_court_downstream_is_frozen(self) -> None:
        hub = self.hub
        hub.schedule_match("m1", "WTO", "R1", "C1", window(t(27, 10), t(27, 12)))
        hub.schedule_match("m2", "WTO", "R2", "C2", window(t(27, 13), t(27, 15)), depends_on=["m1"])
        hub.start_match("m2")  # 下游已开赛（极端情形）
        with self.assertRaises(RuleViolation):
            hub.reschedule_due_to_disruption(
                ["m1"], {"m2": ("C1", window(t(27, 16), t(27, 18)))}, reason="超时"
            )

    def test_respect_other_tournament_confirmed_court(self) -> None:
        hub = self.hub
        hub.schedule_match("m1", "WTO", "R1", "C1", window(t(27, 10), t(27, 12)))
        holder = hub.propose_commitment(
            "MCH", ResourceKind.COURT, "C2", window(t(27, 14), t(27, 16)),
            purpose="男挑已占", proposed_by="scheduler2",
        )
        hub.confirm_commitment(holder.commitment_id, "li")
        with self.assertRaises(ConflictError):
            hub.reschedule_due_to_disruption(
                ["m1"], {"m1": ("C2", window(t(27, 14, 30), t(27, 16, 30)))}, reason="超时"
            )

    def test_delayed_final_cannot_keep_original_teardown_plan(self) -> None:
        """核心场景：女网末轮延期后，系统不允许仍按原计划拆换，同场双占。"""
        hub = self.hub
        women_cfg = VenueConfig("女子赛配置", True, "Z-A", "通道A")
        men_cfg = VenueConfig("男挑赛配置", False, "Z-B", "通道B")

        final_cmt = hub.schedule_match(
            "wom-final", "WTO", "女单末轮", "C1",
            window(t(27, 10), t(27, 13)), config=women_cfg,
        )
        hub.confirm_commitment(final_cmt.commitment_id, "zhang")
        change_cmt = hub.prepare_config_change(
            "cfg-1", "MCH", "C1", women_cfg, men_cfg,
            window(t(27, 13), t(27, 14)), prepared_by="场务组长",
        )
        hub.confirm_commitment(change_cmt.commitment_id, "li")
        q1_cmt = hub.schedule_match(
            "q1", "MCH", "资格赛R1", "C1",
            window(t(27, 14), t(27, 16)), config=men_cfg,
        )

        # 末轮未开赛即遇雨，想把末轮直接顺延进原拆换窗 -> 被已确认的转换作业挡住
        with self.assertRaises(ConflictError):
            hub.reschedule_due_to_disruption(
                ["wom-final"],
                {"wom-final": ("C1", window(t(27, 12, 30), t(27, 14, 30)))},
                reason="降雨",
            )

        # 正确处置：先延后尚未检查的拆换，再把资格赛顺延到转换之后
        delayed = hub.delay_config_change(
            "cfg-1", window(t(27, 14), t(27, 15)), by="li", reason="女网末轮降雨延期"
        )
        hub.confirm_commitment(delayed.commitment_id, "li")
        # 原资格赛建议与拆换窗重叠，已在拆换确认时自动转入候补
        self.assertEqual(hub.commitments[q1_cmt.commitment_id].state, CommitmentState.WAITING)
        hub.reschedule_due_to_disruption(
            ["q1"], {"q1": ("C1", window(t(27, 15), t(27, 17)))}, reason="转换顺延"
        )
        new_q1 = [c for c in hub.commitments.values() if c.match_id == "q1" and c.state == CommitmentState.PROPOSED][0]
        hub.confirm_commitment(new_q1.commitment_id, "li")
        self.assertEqual(hub.matches["q1"].window.start, t(27, 15))
        # 雨停后末轮开赛并完赛（开球推迟但仍赶在拆换前结束），成绩/动线成立；
        # 已完赛内容永不可被后续重排改写
        advance(hub, t(27, 10, 20))
        hub.start_match("wom-final")
        advance(hub, t(27, 13, 50))
        hub.complete_match("wom-final", "6-3 7-6", "w1")
        advance(hub, t(27, 14, 20))
        with self.assertRaises(RuleViolation):
            hub.reschedule_due_to_disruption(
                ["wom-final"], {"wom-final": ("C2", window(t(27, 18), t(27, 20)))}, reason="二次降雨"
            )


class HandoverEndToEndTests(unittest.TestCase):
    def test_women_to_challenger_turnover_report(self) -> None:
        path = tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False).name
        hub = make_hub(path, now=t(27, 7))
        seed_basic(hub)

        women_cfg = VenueConfig("女子赛配置", electronic_lines=True, hvac_zone="Z-A", player_channel="通道A")
        men_cfg = VenueConfig("男挑赛配置", electronic_lines=False, hvac_zone="Z-B", player_channel="通道B")

        # 女网末轮与依赖它的表演活动之外：男挑资格赛依赖女网结束后的配置转换
        hub.schedule_match(
            "wom-final", "WTO", "女单末轮", "C1",
            window(t(27, 10), t(27, 13)), config=women_cfg,
            player_ids=["w1", "w2"], requires_equipment=["ROOF"],
        )
        hub.set_equipment_status("ROOF", True, "屋盖例行检查完成")
        # 配置转换：13:00–14:00 女网配置 → 男挑配置
        change = hub.prepare_config_change(
            "cfg-1", "MCH", "C1", women_cfg, men_cfg,
            window(t(27, 13), t(27, 14)), prepared_by="场务组长",
            deadline=t(27, 9),
        )
        # 男挑资格赛在转换之后，依赖一场前序资格赛
        hub.schedule_match(
            "q-pre", "MCH", "资格赛预选", "C2",
            window(t(27, 12), t(27, 13, 30)),
        )
        hub.schedule_match(
            "q1", "MCH", "资格赛第1轮", "C1",
            window(t(27, 14), t(27, 16)),
            depends_on=["q-pre"], config=men_cfg,
            player_ids=["m1", "m2"],
        )

        advance(hub, t(27, 7, 30))
        hub.confirm_commitment(change.commitment_id, "li")
        women_cmt = next(c for c in hub.commitments.values() if c.match_id == "wom-final")
        qpre_cmt = next(c for c in hub.commitments.values() if c.match_id == "q-pre")
        q1_cmt = next(c for c in hub.commitments.values() if c.match_id == "q1")
        hub.confirm_commitment(women_cmt.commitment_id, "zhang")
        hub.confirm_commitment(qpre_cmt.commitment_id, "li")

        # 交接清单（转换尚未检查、q-pre 未打）：应指出阻塞
        handover = build_handover(hub, now=t(27, 8))
        blocked_ids = {view.match_id for view in handover.blocked_matches}
        self.assertIn("q1", blocked_ids)
        q1_view = next(view for view in handover.blocked_matches if view.match_id == "q1")
        self.assertTrue(any("配置" in reason for reason in q1_view.reasons))
        self.assertTrue(any("q-pre" in reason for reason in q1_view.reasons))
        transition = handover.config_transitions[0]
        self.assertEqual((transition.from_name, transition.to_name), ("女子赛配置", "男挑赛配置"))
        self.assertIsNone(transition.checked_by)
        # 男挑值班长无权确认对方赛事的场地建议
        with self.assertRaises(AuthorizationError):
            hub.confirm_commitment(q1_cmt.commitment_id, "zhang")

        # 女网末轮实际进行并完赛；超时风险下其成绩与动线随后不可被重排改写
        advance(hub, t(27, 9, 55))
        hub.start_match("wom-final")
        hub.lock_player_movement("wom-final", "通道A→中心球场热身区")
        advance(hub, t(27, 13, 5))
        hub.complete_match("wom-final", "7-6(4) 6-3", "w1")
        hub.release_venue("C1", "zhang")

        # 13:00–14:00 拆换电子司线/空调分区，场务完成配置检查
        hub.check_config("cfg-1", "li", True, "电子司线已拆换、空调分区切换")
        # 前序资格赛在 C2 打完
        advance(hub, t(27, 11, 55))
        hub.start_match("q-pre")
        advance(hub, t(27, 13, 30))
        hub.complete_match("q-pre", "6-0 6-0", "m9")

        handover2 = build_handover(hub, now=t(27, 13, 31))
        blocked2 = {view.match_id for view in handover2.blocked_matches}
        self.assertNotIn("q1", blocked2)
        done = next(v for v in handover2.config_transitions if v.court_id == "C1")
        self.assertEqual(done.checked_by, "li")
        self.assertTrue(done.passed)

        # 即使随后降雨，已结束的女网末轮不得重排
        with self.assertRaises(RuleViolation):
            hub.reschedule_due_to_disruption(
                ["wom-final"], {"wom-final": ("C2", window(t(27, 15), t(27, 17)))}, reason="降雨"
            )

        text = render_text(handover2)
        self.assertIn("女子赛配置 → 男挑赛配置", text)
        self.assertIn("检查人 li（已通过）", text)
        self.assertIn("C1 由 zhang", text)

    def test_restart_reconstructs_full_handover(self) -> None:
        path = tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False).name
        hub = make_hub(path, now=t(27, 7))
        seed_basic(hub)
        women_cfg = VenueConfig("女子赛配置", True, "Z-A", "通道A")
        men_cfg = VenueConfig("男挑赛配置", False, "Z-B", "通道B")
        hub.prepare_config_change(
            "cfg-x", "MCH", "C1", women_cfg, men_cfg,
            window(t(27, 13), t(27, 14)), prepared_by="场务组长",
        )
        hub.restore()  # 同存储再次重放不应产生重复或报错
        handover = build_handover(hub, now=t(27, 8))
        self.assertEqual(len(handover.config_transitions), 1)


if __name__ == "__main__":
    unittest.main()
