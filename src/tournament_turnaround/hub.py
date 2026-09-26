"""赛事转换中枢。

基于仅追加事件流：每条命令先做鉴权与规则判定，再以聚合版本乐观锁原子
追加事件，最后投影到内存状态。重启时重放事件流即可恢复全部带版本承诺，
待确认安排仍按事件中保留的原始截止时间处理。

两阶段占用：
  排程建议（propose）→ PROPOSED 软暂存（冲突者进入候补）→ 值班长确认
  （confirm）→ CONFIRMED 正式占用；拒绝/过期/重排 → RELEASED，并按
  可解释顺序推进候补。
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from . import rules
from .events import AggregateType, EventType, make_event
from .model import (
    Commitment,
    CommitmentState,
    DrawStage,
    Entry,
    EntryStatus,
    MatchRecord,
    MatchState,
    Resource,
    ResourceKind,
    TimeWindow,
    VenueConfig,
)
from .store import JsonlEventStore

PLAN_AGGREGATE_ID = "turnaround-plan"


class HubError(Exception):
    """中枢命令被拒绝的基类，原因可直接展示给值班长。"""


class NotFoundError(HubError):
    pass


class AuthorizationError(HubError):
    pass


class ConflictError(HubError):
    pass


class RuleViolation(HubError):
    pass


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _cfg_to_dict(config: VenueConfig | None) -> dict[str, Any] | None:
    if config is None:
        return None
    return {
        "name": config.name,
        "electronic_lines": config.electronic_lines,
        "hvac_zone": config.hvac_zone,
        "player_channel": config.player_channel,
    }


def _cfg_from_dict(data: dict[str, Any] | None) -> VenueConfig | None:
    if data is None:
        return None
    return VenueConfig(
        name=data["name"],
        electronic_lines=data["electronic_lines"],
        hvac_zone=data["hvac_zone"],
        player_channel=data["player_channel"],
    )


def _commitment_to_dict(item: Commitment) -> dict[str, Any]:
    return {
        "commitment_id": item.commitment_id,
        "tournament_id": item.tournament_id,
        "kind": item.kind.value,
        "resource_id": item.resource_id,
        "start": item.window.start.isoformat(),
        "end": item.window.end.isoformat(),
        "state": item.state.value,
        "proposed_by": item.proposed_by,
        "proposed_at": item.proposed_at.isoformat(),
        "plan_id": item.plan_id,
        "confirmed_by": item.confirmed_by,
        "confirmed_at": item.confirmed_at.isoformat() if item.confirmed_at else None,
        "deadline": item.deadline.isoformat() if item.deadline else None,
        "waitlist_seq": item.waitlist_seq,
        "purpose": item.purpose,
        "match_id": item.match_id,
        "config": _cfg_to_dict(item.config),
        "from_config": _cfg_to_dict(item.from_config),
        "reject_reason": item.reject_reason,
        "replaced_by": item.replaced_by,
    }


def _commitment_from_dict(data: dict[str, Any]) -> Commitment:
    return Commitment(
        commitment_id=data["commitment_id"],
        tournament_id=data["tournament_id"],
        kind=ResourceKind(data["kind"]),
        resource_id=data["resource_id"],
        window=TimeWindow(_dt(data["start"]), _dt(data["end"])),
        state=CommitmentState(data["state"]),
        proposed_by=data["proposed_by"],
        proposed_at=_dt(data["proposed_at"]),
        plan_id=data.get("plan_id"),
        confirmed_by=data.get("confirmed_by"),
        confirmed_at=_dt(data["confirmed_at"]) if data.get("confirmed_at") else None,
        deadline=_dt(data["deadline"]) if data.get("deadline") else None,
        waitlist_seq=data.get("waitlist_seq"),
        purpose=data.get("purpose", ""),
        match_id=data.get("match_id"),
        config=_cfg_from_dict(data.get("config")),
        from_config=_cfg_from_dict(data.get("from_config")),
        reject_reason=data.get("reject_reason"),
        replaced_by=data.get("replaced_by"),
    )


class TurnaroundHub:
    def __init__(
        self,
        store: JsonlEventStore,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.store = store
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._versions: dict[tuple[str, str], int] = {}
        # 投影状态
        self.tournaments: dict[str, dict[str, Any]] = {}
        self.draw_stages: dict[str, DrawStage] = {}
        self.entries: dict[str, Entry] = {}
        self._alternates: dict[tuple[str, DrawStage], list[str]] = defaultdict(list)
        self.matches: dict[str, MatchRecord] = {}
        self.movements: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.resources: dict[str, Resource] = {}
        self.commitments: dict[str, Commitment] = {}
        self.wait_seqs: dict[str, int] = defaultdict(int)
        self.config_changes: dict[str, dict[str, Any]] = {}
        self.equipment: dict[str, dict[str, Any]] = {}
        self.training_requests: dict[str, dict[str, Any]] = {}
        self.shifts: dict[str, dict[str, Any]] = {}
        self.security_zones: dict[str, dict[str, Any]] = {}
        self.venue_releases: list[dict[str, Any]] = []
        self._tournament_seq = 0

    # ---- 启动恢复 ------------------------------------------------------

    def restore(self) -> None:
        """重放事件流重建状态，并沿用原截止时间处理已过期的待确认安排。"""
        with self._lock:
            self._reset_projection()
            for event in self.store.load():
                self._apply(event)
            self._expire_due(self.clock())

    def sweep_expired(self) -> list[str]:
        """运行期主动清理超过确认截止的待确认/候补承诺，返回被释放承诺 id。"""
        with self._lock:
            now = self.clock()
            targets = [
                c.commitment_id for c in self.commitments.values()
                if c.state in (CommitmentState.PROPOSED, CommitmentState.HELD, CommitmentState.WAITING)
                and c.deadline is not None and c.deadline <= now
            ]
            self._expire_due(now)
            return targets

    def _reset_projection(self) -> None:
        self._versions = {}
        self.tournaments = {}
        self.draw_stages = {}
        self.entries = {}
        self._alternates = defaultdict(list)
        self.matches = {}
        self.movements = defaultdict(list)
        self.resources = {}
        self.commitments = {}
        self.wait_seqs = defaultdict(int)
        self.config_changes = {}
        self.equipment = {}
        self.training_requests = {}
        self.shifts = {}
        self.security_zones = {}
        self.venue_releases = []
        self._tournament_seq = 0

    # ---- 赛事与资源登记 -------------------------------------------------

    def register_tournament(
        self, tournament_id: str, name: str, duty_officers: Iterable[str], *, priority: int | None = None
    ) -> None:
        with self._lock:
            if tournament_id in self.tournaments:
                raise RuleViolation(f"赛事 {tournament_id} 已登记")
            self._tournament_seq += 1
            data = {
                "name": name,
                "duty_officers": sorted(set(duty_officers)),
                "priority": self._tournament_seq if priority is None else priority,
            }
            self._emit(
                EventType.TOURNAMENT_REGISTERED, AggregateType.TOURNAMENT, tournament_id,
                f"登记赛事 {name}", data,
            )

    def register_resource(self, resource_id: str, kind: ResourceKind, label: str) -> None:
        with self._lock:
            key = self._resource_key(kind, resource_id)
            if key in self.resources:
                raise RuleViolation(f"资源 {key} 已登记")
            self._emit(
                EventType.RESOURCE_REGISTERED, AggregateType.VENUE_RESOURCE, key,
                f"登记资源 {label}", {"kind": kind.value, "resource_id": resource_id, "label": label},
            )

    # ---- 名单、退赛与递补 ----------------------------------------------

    def confirm_entry(
        self,
        entry_id: str,
        tournament_id: str,
        player_id: str,
        *,
        stage: DrawStage | None = None,
        occupies_slot: str | None = None,
    ) -> None:
        """登记参赛名单。occupies_slot 为空则列为该阶段候补。"""
        with self._lock:
            self._require_tournament(tournament_id)
            if entry_id in self.entries:
                raise RuleViolation(f"报名记录 {entry_id} 已存在")
            current_stage = self.draw_stages.get(tournament_id, DrawStage.ENTRY)
            stage = stage or current_stage
            status = EntryStatus.IN if occupies_slot else EntryStatus.ALTERNATE
            data = {
                "tournament_id": tournament_id,
                "player_id": player_id,
                "draw_stage": stage.value,
                "occupies_slot": occupies_slot,
                "status": status.value,
                "accepted_at": self.clock().isoformat(),
            }
            self._emit(
                EventType.ENTRY_CONFIRMED, AggregateType.ENTRY, entry_id,
                f"{player_id} 进入{tournament_id}名单", data,
            )

    def set_draw_stage(self, tournament_id: str, stage: DrawStage) -> None:
        with self._lock:
            self._require_tournament(tournament_id)
            current = self.draw_stages.get(tournament_id, DrawStage.ENTRY)
            order = list(DrawStage)
            if order.index(stage) < order.index(current):
                raise RuleViolation(f"签表阶段只能向前推进：当前 {current.value}，请求 {stage.value}")
            self._emit(
                EventType.DRAW_STAGE_SET, AggregateType.DRAW, f"draw:{tournament_id}",
                f"{tournament_id} 签表进入 {stage.value}", {"tournament_id": tournament_id, "stage": stage.value},
            )

    def withdraw(self, entry_id: str) -> str | None:
        """退赛；若腾出签位且当前阶段存在合格候补，按规则自动递补。

        返回递补者 entry_id；无合格候补时返回 None（签位暂空，列入交接说明）。
        """
        with self._lock:
            entry = self._require_entry(entry_id)
            if entry.status == EntryStatus.WITHDRAWN:
                raise RuleViolation(f"{entry_id} 已退赛")
            tournament_id = entry.tournament_id
            self._emit(
                EventType.ENTRY_WITHDRAWN, AggregateType.ENTRY, entry_id,
                f"{entry.player_id} 退出 {tournament_id}", {"at": self.clock().isoformat()},
            )
            if not entry.occupies_slot:
                return None
            return self._auto_replace(entry_id)

    def replace_withdrawn(self, withdrawn_entry_id: str, candidate_entry_id: str) -> str:
        """显式指定候补递补，仍须通过当时签表规则校验。"""
        with self._lock:
            withdrawn = self._require_entry(withdrawn_entry_id)
            candidate = self._require_entry(candidate_entry_id)
            stage = self.draw_stages.get(withdrawn.tournament_id, DrawStage.ENTRY)
            ok, reason = rules.can_replace(
                withdrawn=withdrawn,
                candidate=candidate,
                occupied_slots=rules.slots_occupied(list(self.entries.values())),
                current_stage=stage,
            )
            if not ok:
                raise RuleViolation(f"递补被拒：{reason}")
            self._do_replace(withdrawn, candidate)
            return candidate.entry_id

    def _auto_replace(self, withdrawn_entry_id: str) -> str | None:
        withdrawn = self.entries[withdrawn_entry_id]
        stage = self.draw_stages.get(withdrawn.tournament_id, DrawStage.ENTRY)
        occupied = rules.slots_occupied(list(self.entries.values()))
        for candidate_id in list(self._alternates[(withdrawn.tournament_id, stage)]):
            candidate = self.entries[candidate_id]
            ok, _ = rules.can_replace(
                withdrawn=withdrawn, candidate=candidate, occupied_slots=occupied, current_stage=stage
            )
            if ok:
                self._do_replace(withdrawn, candidate)
                return candidate_id
        return None

    def _do_replace(self, withdrawn: Entry, candidate: Entry) -> None:
        """candidate 接管 withdrawn 的签位；候选记录本身转为递补状态。"""
        self._emit(
            EventType.ENTRY_REPLACED, AggregateType.ENTRY, candidate.entry_id,
            f"{candidate.player_id} 递补 {withdrawn.player_id} 的签位 {withdrawn.occupies_slot}",
            {
                "tournament_id": withdrawn.tournament_id,
                "replaced_entry_id": withdrawn.entry_id,
                "player_id": candidate.player_id,
                "slot": withdrawn.occupies_slot,
                "draw_stage": self.draw_stages.get(withdrawn.tournament_id, DrawStage.ENTRY).value,
                "at": self.clock().isoformat(),
            },
        )

    # ---- 比赛：排程、开赛、成绩、动线、重排 ------------------------------

    def schedule_match(
        self,
        match_id: str,
        tournament_id: str,
        round_name: str,
        court_id: str,
        window: TimeWindow,
        *,
        depends_on: Iterable[str] = (),
        config: VenueConfig | None = None,
        player_ids: Iterable[str] = (),
        requires_equipment: Iterable[str] = (),
        proposed_by: str | None = None,
        deadline: datetime | None = None,
    ) -> Commitment:
        """初排比赛：创建未开赛比赛记录，并就场地提出带截止的排程建议。"""
        with self._lock:
            self._require_tournament(tournament_id)
            if match_id in self.matches:
                raise RuleViolation(f"比赛 {match_id} 已存在")
            for prerequisite in depends_on:
                if prerequisite not in self.matches:
                    raise RuleViolation(f"前序比赛 {prerequisite} 尚未登记")
            court_key = self._resource_key(ResourceKind.COURT, court_id)
            if court_key not in self.resources:
                raise NotFoundError(f"场地资源 {court_key} 尚未登记")
            data = {
                "tournament_id": tournament_id,
                "round_name": round_name,
                "court_id": court_id,
                "start": window.start.isoformat(),
                "end": window.end.isoformat(),
                "depends_on": tuple(depends_on),
                "config": _cfg_to_dict(config),
                "player_ids": tuple(player_ids),
                "requires_equipment": tuple(requires_equipment),
                "schedule_version": 1,
            }
            self._emit(
                EventType.MATCH_SCHEDULED, AggregateType.MATCH, match_id,
                f"{tournament_id} {round_name} 初排至 {court_id}", data,
            )
            return self._propose_commitment(
                tournament_id=tournament_id,
                kind=ResourceKind.COURT,
                resource_id=court_id,
                window=window,
                purpose=f"比赛 {match_id}",
                match_id=match_id,
                config=config,
                proposed_by=proposed_by or "排程系统",
                deadline=deadline,
            )

    def start_match(self, match_id: str) -> None:
        with self._lock:
            record = self._require_match(match_id)
            if record.state != MatchState.NOT_STARTED:
                raise RuleViolation(f"比赛 {match_id} 状态为 {record.state.value}，不能开赛")
            self._emit(
                EventType.MATCH_STARTED, AggregateType.MATCH, match_id,
                f"{match_id} 开赛", {"at": self.clock().isoformat()},
            )

    def complete_match(self, match_id: str, score: str, winner_id: str) -> None:
        with self._lock:
            record = self._require_match(match_id)
            if record.state == MatchState.COMPLETED:
                raise RuleViolation(f"比赛 {match_id} 已结束，成绩不可改写")
            if record.state != MatchState.ON_COURT:
                raise RuleViolation(f"比赛 {match_id} 尚未开赛，不能记录成绩")
            self._emit(
                EventType.MATCH_COMPLETED, AggregateType.MATCH, match_id,
                f"{match_id} 结束，{winner_id} 胜",
                {"score": score, "winner_id": winner_id, "at": self.clock().isoformat()},
            )

    def lock_player_movement(self, match_id: str, route: str, player_ids: Iterable[str] | None = None) -> None:
        """记录球员实际动线（不可变事实）；仅进行中或已结束的比赛可登记。"""
        with self._lock:
            record = self._require_match(match_id)
            if record.state not in (MatchState.ON_COURT, MatchState.COMPLETED):
                raise RuleViolation("实际动线只能对进行中或已结束的比赛登记")
            self._emit(
                EventType.PLAYER_MOVEMENT_LOCKED, AggregateType.MATCH, match_id,
                f"{match_id} 球员动线已锁定：{route}",
                {"route": route, "player_ids": tuple(player_ids or record.player_ids), "at": self.clock().isoformat()},
            )

    def reschedule_due_to_disruption(
        self,
        root_match_ids: Iterable[str],
        new_slots: dict[str, tuple[str, TimeWindow]],
        *,
        reason: str,
    ) -> list[Commitment]:
        """降雨或比赛超时后的重排。

        - 只能触及以 root 为起点的依赖链，且链上比赛必须尚未开赛；
          已结束成绩、进行中比赛与已登记动线绝不改写。
        - 被重排比赛的原场地承诺释放（候补随之推进），按新时段重新提出建议。
        """
        with self._lock:
            roots = list(root_match_ids)
            for root in roots:
                self._require_match(root)
            allowed, frozen = rules.reschedulable(roots, self.matches)
            for match_id in new_slots:
                if match_id not in allowed:
                    state = self.matches[match_id].state.value if match_id in self.matches else "不存在"
                    raise RuleViolation(
                        f"{match_id} 不在可重排范围（状态 {state}）："
                        "已结束成绩与进行中动线不得改写"
                    )
            # 新时段之间也不得自相重叠（同一块场地）
            new_windows: dict[str, list[TimeWindow]] = defaultdict(list)
            for match_id, (court_id, window) in new_slots.items():
                if window.start < self.clock():
                    raise RuleViolation(f"{match_id} 新排程开始时间已过")
                for other_window in new_windows[court_id]:
                    if rules.windows_overlap(window, other_window):
                        raise RuleViolation(f"{match_id} 与另一重排比赛在 {court_id} 新时段自相重叠")
                new_windows[court_id].append(window)
            # 不得强排到已确认占用的时段（被重排比赛自身的旧占用除外）
            moving = set(new_slots)
            for match_id, (court_id, window) in new_slots.items():
                tournament_id = self.matches[match_id].tournament_id
                for item in self._active_on(ResourceKind.COURT, self._resource_key(ResourceKind.COURT, court_id)):
                    if item.state != CommitmentState.CONFIRMED:
                        continue
                    if item.match_id in moving:
                        continue
                    if rules.windows_overlap(window, item.window):
                        raise ConflictError(
                            f"{match_id} 新时段与 {item.tournament_id} 已确认占用冲突（{item.purpose}）"
                        )
            results: list[Commitment] = []
            for match_id, (court_id, window) in new_slots.items():
                record = self.matches[match_id]
                old_court_key = self._resource_key(ResourceKind.COURT, record.court_id or court_id)
                # 释放该比赛原有的所有未终结场地承诺（含候补中的旧建议，
                # 避免重排后残留过时候补），候补稍后统一推进
                for item in list(self.commitments.values()):
                    if (
                        item.match_id == match_id
                        and item.state in (
                            CommitmentState.PROPOSED, CommitmentState.HELD,
                            CommitmentState.CONFIRMED, CommitmentState.WAITING,
                        )
                    ):
                        self._release(item.commitment_id, reason=f"因{reason}重排", advance=False)
                self._emit(
                    EventType.MATCH_SCHEDULED, AggregateType.MATCH, match_id,
                    f"{match_id} 因{reason}重排至 {court_id} {window.start.isoformat()}",
                    {
                        "tournament_id": record.tournament_id,
                        "round_name": record.round_name,
                        "court_id": court_id,
                        "start": window.start.isoformat(),
                        "end": window.end.isoformat(),
                        "depends_on": record.depends_on,
                        "config": _cfg_to_dict(record.config),
                        "player_ids": record.player_ids,
                        "requires_equipment": record.requires_equipment,
                        "schedule_version": record.schedule_version + 1,
                    },
                )
                # 原场地释放后给既有候补一次机会
                self._advance_waitlist(ResourceKind.COURT, old_court_key)
                results.append(
                    self._propose_commitment(
                        tournament_id=record.tournament_id,
                        kind=ResourceKind.COURT,
                        resource_id=court_id,
                        window=window,
                        purpose=f"比赛 {match_id}（重排）",
                        match_id=match_id,
                        config=record.config,
                        proposed_by="排程系统",
                    )
                )
            return results

    # ---- 训练、配置转换、设备、班次、安保 ---------------------------------

    def request_training(
        self,
        request_id: str,
        tournament_id: str,
        court_id: str,
        window: TimeWindow,
        player_ids: Iterable[str],
        *,
        proposed_by: str,
        deadline: datetime | None = None,
    ) -> Commitment:
        with self._lock:
            self._require_tournament(tournament_id)
            court_key = self._resource_key(ResourceKind.COURT, court_id)
            if court_key not in self.resources:
                raise NotFoundError(f"场地资源 {court_key} 尚未登记")
            if request_id in self.training_requests:
                raise RuleViolation(f"训练申请 {request_id} 已存在")
            players = tuple(player_ids)
            self._emit(
                EventType.TRAINING_REQUESTED, AggregateType.TRAINING_REQUEST, request_id,
                f"{tournament_id} 申请 {court_id} 训练时段",
                {
                    "tournament_id": tournament_id,
                    "court_id": court_id,
                    "start": window.start.isoformat(),
                    "end": window.end.isoformat(),
                    "player_ids": players,
                    "at": self.clock().isoformat(),
                },
            )
            return self._propose_commitment(
                tournament_id=tournament_id,
                kind=ResourceKind.COURT,
                resource_id=court_id,
                window=window,
                purpose=f"训练 {request_id}",
                proposed_by=proposed_by,
                deadline=deadline,
            )

    def prepare_config_change(
        self,
        change_id: str,
        tournament_id: str,
        court_id: str,
        from_config: VenueConfig,
        to_config: VenueConfig,
        window: TimeWindow,
        *,
        prepared_by: str,
        deadline: datetime | None = None,
    ) -> Commitment:
        """登记一块场地从何种配置转到下一种配置的转换作业（含检查承诺）。"""
        with self._lock:
            self._require_tournament(tournament_id)
            court_key = self._resource_key(ResourceKind.COURT, court_id)
            if court_key not in self.resources:
                raise NotFoundError(f"场地资源 {court_key} 尚未登记")
            if change_id in self.config_changes:
                raise RuleViolation(f"配置转换 {change_id} 已登记")
            self._emit(
                EventType.VENUE_CONFIG_PREPARED, AggregateType.CONFIG_CHANGE, change_id,
                f"{court_id}：{from_config.name} → {to_config.name}",
                {
                    "tournament_id": tournament_id,
                    "court_id": court_id,
                    "from_config": _cfg_to_dict(from_config),
                    "to_config": _cfg_to_dict(to_config),
                    "start": window.start.isoformat(),
                    "end": window.end.isoformat(),
                    "prepared_by": prepared_by,
                    "at": self.clock().isoformat(),
                },
            )
            # 转换作业物理上占用该球场：与比赛承诺同一资源命名空间，
            # 末轮一旦被重排进转换窗就会被已确认占用挡住，不会"同时占用"。
            return self._propose_commitment(
                tournament_id=tournament_id,
                kind=ResourceKind.COURT,
                resource_id=court_id,
                window=window,
                purpose=f"配置转换 {change_id}：{from_config.name}→{to_config.name}",
                config=to_config,
                from_config=from_config,
                proposed_by=prepared_by,
                deadline=deadline,
            )

    def check_config(self, change_id: str, officer: str, passed: bool, notes: str = "") -> None:
        with self._lock:
            change = self.config_changes.get(change_id)
            if change is None:
                raise NotFoundError(f"配置转换 {change_id} 不存在")
            self._require_officer(change["tournament_id"], officer)
            if change.get("checked_by"):
                raise RuleViolation(f"配置转换 {change_id} 已由 {change['checked_by']} 完成检查")
            self._emit(
                EventType.VENUE_CONFIG_CHECKED, AggregateType.CONFIG_CHANGE, change_id,
                f"{change_id} 检查{'通过' if passed else '未通过'}（{officer}）",
                {"checked_by": officer, "passed": passed, "notes": notes, "at": self.clock().isoformat()},
            )

    def delay_config_change(self, change_id: str, new_window: TimeWindow, *, by: str, reason: str) -> Commitment:
        """前序赛事超时/降雨时延后尚未开始的配置转换作业。

        已完成检查的转换视为既成事实，不得改期；作业承诺释放后候补照常推进，
        再按新窗口提出建议。其下游未开赛比赛由调用方经
        ``reschedule_due_to_disruption`` 连带重排。
        """
        with self._lock:
            change = self.config_changes.get(change_id)
            if change is None:
                raise NotFoundError(f"配置转换 {change_id} 不存在")
            self._require_officer(change["tournament_id"], by)
            if change.get("checked_by"):
                raise RuleViolation(f"配置转换 {change_id} 已完成检查，不能改期")
            if new_window.start < self.clock():
                raise RuleViolation("新转换窗口开始时间已过")
            court_id = change["court_id"]
            for item in list(self.commitments.values()):
                if change_id in item.purpose and item.state in (
                    CommitmentState.PROPOSED, CommitmentState.HELD, CommitmentState.CONFIRMED
                ):
                    self._release(item.commitment_id, reason=f"{by}：{reason}", advance=False)
            self._emit(
                EventType.VENUE_CONFIG_PREPARED, AggregateType.CONFIG_CHANGE, change_id,
                f"{court_id}：{change['from_config'].name} → {change['to_config'].name}（改期）",
                {
                    "tournament_id": change["tournament_id"],
                    "court_id": court_id,
                    "from_config": _cfg_to_dict(change["from_config"]),
                    "to_config": _cfg_to_dict(change["to_config"]),
                    "start": new_window.start.isoformat(),
                    "end": new_window.end.isoformat(),
                    "prepared_by": change["prepared_by"],
                    "at": self.clock().isoformat(),
                },
            )
            self._advance_waitlist(
                ResourceKind.COURT, self._resource_key(ResourceKind.COURT, court_id)
            )
            return self._propose_commitment(
                tournament_id=change["tournament_id"],
                kind=ResourceKind.COURT,
                resource_id=court_id,
                window=new_window,
                purpose=f"配置转换 {change_id}：{change['from_config'].name}→{change['to_config'].name}（改期）",
                config=change["to_config"],
                from_config=change["from_config"],
                proposed_by=change["prepared_by"],
            )

    def set_equipment_status(self, equipment_id: str, available: bool, note: str = "") -> None:
        with self._lock:
            self._emit(
                EventType.ROOF_EQUIPMENT_STATUS, AggregateType.EQUIPMENT, equipment_id,
                f"设备 {equipment_id} {'可用' if available else '不可用'}：{note}",
                {"available": available, "note": note, "at": self.clock().isoformat()},
            )

    def publish_shift(
        self,
        shift_id: str,
        tournament_id: str,
        crew_id: str,
        window: TimeWindow,
        *,
        role: str,
        proposed_by: str,
        deadline: datetime | None = None,
    ) -> Commitment:
        with self._lock:
            self._require_tournament(tournament_id)
            key = self._resource_key(ResourceKind.CREW, crew_id)
            if key not in self.resources:
                self._emit(
                    EventType.RESOURCE_REGISTERED, AggregateType.VENUE_RESOURCE, key,
                    f"登记班组 {crew_id}", {"kind": ResourceKind.CREW.value, "resource_id": crew_id, "label": crew_id},
                )
            if shift_id in self.shifts:
                raise RuleViolation(f"班次 {shift_id} 已发布")
            self._emit(
                EventType.SHIFT_PUBLISHED, AggregateType.SHIFT, shift_id,
                f"{tournament_id} 班组 {crew_id} {role} 班次",
                {
                    "tournament_id": tournament_id,
                    "crew_id": crew_id,
                    "role": role,
                    "start": window.start.isoformat(),
                    "at": self.clock().isoformat(),
                },
            )
            return self._propose_commitment(
                tournament_id=tournament_id,
                kind=ResourceKind.CREW,
                resource_id=crew_id,
                window=window,
                purpose=f"班次 {shift_id}（{role}）",
                proposed_by=proposed_by,
                deadline=deadline,
            )

    def assign_security_zone(
        self,
        zone_id: str,
        tournament_id: str,
        window: TimeWindow,
        *,
        note: str,
        proposed_by: str,
        deadline: datetime | None = None,
    ) -> Commitment:
        with self._lock:
            self._require_tournament(tournament_id)
            zone_key = self._resource_key(ResourceKind.SECURITY, zone_id)
            if zone_key not in self.resources:
                self._emit(
                    EventType.RESOURCE_REGISTERED, AggregateType.VENUE_RESOURCE, zone_key,
                    f"登记安保分区 {zone_id}",
                    {"kind": ResourceKind.SECURITY.value, "resource_id": zone_id, "label": f"安保分区 {zone_id}"},
                )
            if zone_id in self.security_zones:
                raise RuleViolation(f"安保分区 {zone_id} 已分配")
            self._emit(
                EventType.SECURITY_ZONE_ASSIGNED, AggregateType.SECURITY_ZONE, zone_id,
                f"{tournament_id} 安保分区 {zone_id}",
                {
                    "tournament_id": tournament_id,
                    "start": window.start.isoformat(),
                    "end": window.end.isoformat(),
                    "note": note,
                    "at": self.clock().isoformat(),
                },
            )
            return self._propose_commitment(
                tournament_id=tournament_id,
                kind=ResourceKind.SECURITY,
                resource_id=zone_id,
                window=window,
                purpose=f"安保分区 {zone_id}：{note}",
                proposed_by=proposed_by,
                deadline=deadline,
            )

    def release_venue(self, court_id: str, officer: str) -> None:
        """整块场地交还给下一项赛事（交接事实）。"""
        with self._lock:
            self._emit(
                EventType.VENUE_RELEASED, AggregateType.VENUE_RESOURCE,
                self._resource_key(ResourceKind.COURT, court_id),
                f"{court_id} 由 {officer} 交还", {"court_id": court_id, "by": officer, "at": self.clock().isoformat()},
            )

    # ---- 两阶段占用与候补 ----------------------------------------------

    def propose_commitment(
        self,
        tournament_id: str,
        kind: ResourceKind,
        resource_id: str,
        window: TimeWindow,
        *,
        purpose: str,
        proposed_by: str,
        deadline: datetime | None = None,
    ) -> Commitment:
        """通用排程建议（球员通道、设备使用窗等）。"""
        with self._lock:
            self._require_tournament(tournament_id)
            return self._propose_commitment(
                tournament_id=tournament_id,
                kind=kind,
                resource_id=resource_id,
                window=window,
                purpose=purpose,
                proposed_by=proposed_by,
                deadline=deadline,
            )

    def confirm_commitment(self, commitment_id: str, officer: str) -> Commitment:
        """值班长确认排程建议；并发确认同一时段时只有一方成功。

        失败者不报错丢弃，而是带可解释序号进入该资源的候补队列；待占用方
        释放后自动按顺序推进。
        """
        with self._lock:
            item = self._require_commitment(commitment_id)
            self._require_officer(item.tournament_id, officer)
            if item.state == CommitmentState.CONFIRMED:
                raise ConflictError(f"{commitment_id} 已确认（{item.confirmed_by}）")
            if item.state == CommitmentState.WAITING:
                raise ConflictError(f"{commitment_id} 仍在候补，暂不能确认")
            if item.state in (CommitmentState.RELEASED, CommitmentState.REJECTED):
                raise ConflictError(f"{commitment_id} 已{item.state.value}")
            if item.deadline is not None and item.deadline <= self.clock():
                raise ConflictError(f"{commitment_id} 已超过确认截止 {item.deadline.isoformat()}")
            blockers = rules.conflicts_with(
                replace(item, state=CommitmentState.CONFIRMED),
                [c for c in self.commitments.values() if c.state == CommitmentState.CONFIRMED],
            )
            if blockers:
                raise ConflictError(
                    "同一时段已由 "
                    + "；".join(f"{b.tournament_id}:{b.purpose}" for b in blockers)
                    + " 确认占用"
                )
            # 仲裁成功：同一资源上与之重叠的其他排程建议全部转为候补，
            # 按"窗口开始→赛事优先级→建议到达序号"赋序，释放后依序推进。
            extra_events: list[tuple[str, str, str, str, dict[str, Any]]] = []
            rivals = [
                c for c in self.commitments.values()
                if c.commitment_id != commitment_id
                and c.state == CommitmentState.PROPOSED
                and c.resource_id == item.resource_id
                and rules.windows_overlap(c.window, item.window)
            ]
            for rival in sorted(rivals, key=lambda c: (c.window.start, self._priorities().get(c.tournament_id, 1_000_000), c.proposed_at)):
                self.wait_seqs[item.resource_id] += 1
                seq = self.wait_seqs[item.resource_id]
                waiting_rival = replace(rival, state=CommitmentState.WAITING, waitlist_seq=seq)
                self.commitments[rival.commitment_id] = waiting_rival
                extra_events.append((
                    EventType.WAITLIST_JOINED, AggregateType.TURNAROUND_PLAN, PLAN_AGGREGATE_ID,
                    f"{rival.purpose} 仲裁失利转入候补 #{seq}（占用方：{item.purpose}）",
                    {"commitment": _commitment_to_dict(waiting_rival)},
                ))
            self._emit_bulk(extra_events + [(
                EventType.RESOURCE_CONFIRMED, AggregateType.TURNAROUND_PLAN, PLAN_AGGREGATE_ID,
                f"{item.purpose} 经 {officer} 确认",
                {"commitment_id": commitment_id, "by": officer, "at": self.clock().isoformat()},
            )])
            return self.commitments[commitment_id]

    def release_commitment(self, commitment_id: str, by: str, reason: str) -> None:
        with self._lock:
            self._release(commitment_id, reason=f"{by}：{reason}")

    def waiting_commitments(self, kind: ResourceKind, resource_id: str) -> list[Commitment]:
        """按可解释顺序返回某资源的候补队列。"""
        with self._lock:
            key = self._resource_key(kind, resource_id)
            waiting = [c for c in self.commitments.values() if c.resource_id == key and c.state == CommitmentState.WAITING]
            return rules.ordered_waitlist(waiting, self._priorities())

    def pending_confirmation(self) -> list[Commitment]:
        """所有待值班长确认的排程建议（重启后仍按各自截止处理）。"""
        with self._lock:
            return [
                c for c in self.commitments.values()
                if c.state in (CommitmentState.PROPOSED, CommitmentState.HELD)
            ]

    # ---- 内部：占用/候补/截止 -------------------------------------------

    def _propose_commitment(
        self,
        *,
        tournament_id: str,
        kind: ResourceKind,
        resource_id: str,
        window: TimeWindow,
        purpose: str,
        proposed_by: str,
        deadline: datetime | None = None,
        match_id: str | None = None,
        config: VenueConfig | None = None,
        from_config: VenueConfig | None = None,
        plan_id: str | None = None,
    ) -> Commitment:
        key = self._resource_key(kind, resource_id)
        if key not in self.resources:
            raise NotFoundError(f"资源 {key} 尚未登记")
        now = self.clock()
        commitment_id = f"cmt-{kind.value}-{resource_id}-{now.strftime('%Y%m%d%H%M%S%f')}-{len(self.commitments) + 1:04d}"
        active = self._active_on(kind, key)
        waiting_state = bool(rules.conflicts_with(
            Commitment(
                commitment_id=commitment_id,
                tournament_id=tournament_id,
                kind=kind,
                resource_id=key,
                window=window,
                state=CommitmentState.PROPOSED,
                proposed_by=proposed_by,
                proposed_at=now,
            ),
            active,
        ))
        seq: int | None = None
        if waiting_state:
            self.wait_seqs[key] += 1
            seq = self.wait_seqs[key]
        item = Commitment(
            commitment_id=commitment_id,
            tournament_id=tournament_id,
            kind=kind,
            resource_id=key,
            window=window,
            state=CommitmentState.WAITING if waiting_state else CommitmentState.PROPOSED,
            proposed_by=proposed_by,
            proposed_at=now,
            plan_id=plan_id,
            deadline=deadline,
            waitlist_seq=seq,
            purpose=purpose,
            match_id=match_id,
            config=config,
            from_config=from_config,
        )
        event_type = EventType.WAITLIST_JOINED if waiting_state else EventType.RESOURCE_HELD
        summary = f"{purpose} 进入候补 #{seq}" if waiting_state else f"{purpose} 形成待确认承诺"
        if deadline is not None and not waiting_state:
            self._emit_two(
                (event_type, AggregateType.TURNAROUND_PLAN, PLAN_AGGREGATE_ID, summary,
                 {"commitment": _commitment_to_dict(item)}),
                (EventType.DEADLINE_SET, AggregateType.TURNAROUND_PLAN, PLAN_AGGREGATE_ID,
                 f"{purpose} 确认截止 {deadline.isoformat()}",
                 {"commitment_id": commitment_id, "deadline": deadline.isoformat()}),
            )
        else:
            self._emit(
                event_type, AggregateType.TURNAROUND_PLAN, PLAN_AGGREGATE_ID, summary,
                {"commitment": _commitment_to_dict(item)},
            )
        return self.commitments[commitment_id]

    def _release(self, commitment_id: str, *, reason: str, advance: bool = True) -> None:
        item = self._require_commitment(commitment_id)
        if item.state in (CommitmentState.RELEASED, CommitmentState.REJECTED):
            raise RuleViolation(f"{commitment_id} 已释放")
        self._emit(
            EventType.RESOURCE_RELEASED, AggregateType.TURNAROUND_PLAN, PLAN_AGGREGATE_ID,
            f"{item.purpose} 释放：{reason}",
            {"commitment_id": commitment_id, "reason": reason, "at": self.clock().isoformat()},
        )
        if advance:
            self._advance_waitlist(item.kind, item.resource_id)

    def _advance_waitlist(self, kind: ResourceKind, key: str) -> None:
        now = self.clock()
        waiting = [c for c in self.commitments.values() if c.resource_id == key and c.state == CommitmentState.WAITING]
        # 已硬占用（HELD/CONFIRMED）的窗口；本轮刚推进的建议同样占位，保证
        # 一次释放只按顺序放行一个候补，其余继续等待（可解释、不超额）。
        blocking_windows = [c.window for c in self._active_on(kind, key)]
        for item in rules.ordered_waitlist(waiting, self._priorities()):
            if item.deadline is not None and item.deadline <= now:
                # 候补期间已过确认截止：释放并不再推进
                self._emit(
                    EventType.RESOURCE_RELEASED, AggregateType.TURNAROUND_PLAN, PLAN_AGGREGATE_ID,
                    f"{item.purpose} 候补期间超过确认截止，释放",
                    {"commitment_id": item.commitment_id, "reason": "候补期间超过确认截止", "at": now.isoformat()},
                )
                continue
            if any(rules.windows_overlap(item.window, blocked) for blocked in blocking_windows):
                continue
            self._emit(
                EventType.WAITLIST_ADVANCED, AggregateType.TURNAROUND_PLAN, PLAN_AGGREGATE_ID,
                f"{item.purpose} 候补推进为待确认（窗口 {item.window.start.isoformat()}）",
                {"commitment_id": item.commitment_id, "at": now.isoformat()},
            )
            blocking_windows.append(item.window)

    def _expire_due(self, now: datetime) -> None:
        """超过确认截止仍未确认的承诺自动释放，并推进候补。重启时同样执行。"""
        due = [
            c for c in list(self.commitments.values())
            if c.state in (CommitmentState.PROPOSED, CommitmentState.HELD, CommitmentState.WAITING)
            and c.deadline is not None and c.deadline <= now
        ]
        for item in due:
            self._emit(
                EventType.RESOURCE_RELEASED, AggregateType.TURNAROUND_PLAN, PLAN_AGGREGATE_ID,
                f"{item.purpose} 超过确认截止自动释放",
                {"commitment_id": item.commitment_id, "reason": "超过确认截止", "at": now.isoformat()},
            )
            if item.state != CommitmentState.WAITING:
                self._advance_waitlist(item.kind, item.resource_id)

    # ---- 内部：查询辅助 -------------------------------------------------

    def _priorities(self) -> dict[str, int]:
        return {tid: info["priority"] for tid, info in self.tournaments.items()}

    def _active_on(self, kind: ResourceKind, key: str) -> list[Commitment]:
        states = rules.active_commitment_states()
        return [
            c for c in self.commitments.values()
            if c.kind == kind and c.resource_id == key and c.state in states
        ]

    @staticmethod
    def _resource_key(kind: ResourceKind, resource_id: str) -> str:
        return f"{kind.value}:{resource_id}"

    def _require_tournament(self, tournament_id: str) -> dict[str, Any]:
        info = self.tournaments.get(tournament_id)
        if info is None:
            raise NotFoundError(f"赛事 {tournament_id} 未登记")
        return info

    def _require_entry(self, entry_id: str) -> Entry:
        entry = self.entries.get(entry_id)
        if entry is None:
            raise NotFoundError(f"报名记录 {entry_id} 不存在")
        return entry

    def _require_match(self, match_id: str) -> MatchRecord:
        record = self.matches.get(match_id)
        if record is None:
            raise NotFoundError(f"比赛 {match_id} 不存在")
        return record

    def _require_commitment(self, commitment_id: str) -> Commitment:
        item = self.commitments.get(commitment_id)
        if item is None:
            raise NotFoundError(f"承诺 {commitment_id} 不存在")
        return item

    def _require_officer(self, tournament_id: str, officer: str) -> None:
        info = self._require_tournament(tournament_id)
        if officer not in info["duty_officers"]:
            raise AuthorizationError(f"{officer} 不是 {tournament_id}（{info['name']}）的值班长")

    # ---- 内部：事件追加与投影 -------------------------------------------

    def _emit(self, event_type: str, aggregate_type: str, aggregate_id: str, summary: str, data: dict[str, Any]) -> None:
        self._emit_bulk([(event_type, aggregate_type, aggregate_id, summary, data)])

    def _emit_two(self, first: tuple, second: tuple) -> None:
        self._emit_bulk([first, second])

    def _emit_bulk(self, specs: list[tuple[str, str, str, str, dict[str, Any]]]) -> None:
        events: list[dict[str, Any]] = []
        expects: dict[tuple[str, str], int] = {}
        next_versions: dict[tuple[str, str], int] = {}
        now = self.clock()
        for event_type, aggregate_type, aggregate_id, summary, data in specs:
            key = (aggregate_type, aggregate_id)
            if key not in expects:
                expects[key] = self._versions.get(key, 0)
                next_versions[key] = self._versions.get(key, 0)
            next_versions[key] += 1
            events.append(make_event(
                event_type=event_type,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                occurred_at=now,
                version=next_versions[key],
                summary=summary,
                data=data,
            ))
        self.store.append_many(events, expected_versions=expects)
        for event in events:
            self._apply(event)

    def _apply(self, event: dict[str, Any]) -> None:  # noqa: C901 - 投影分发
        key = (event["aggregate_type"], event["aggregate_id"])
        self._versions[key] = event["version"]
        etype = event["event_type"]
        data = event.get("data", {})
        at = _dt(event["occurred_at"])

        if etype == EventType.TOURNAMENT_REGISTERED:
            self._tournament_seq = max(self._tournament_seq, data["priority"])
            self.tournaments[event["aggregate_id"]] = {
                "name": data["name"],
                "duty_officers": set(data["duty_officers"]),
                "priority": data["priority"],
            }
        elif etype == EventType.RESOURCE_REGISTERED:
            kind = ResourceKind(data["kind"])
            self.resources[event["aggregate_id"]] = Resource(event["aggregate_id"], kind, data["label"])
        elif etype == EventType.DRAW_STAGE_SET:
            self.draw_stages[data["tournament_id"]] = DrawStage(data["stage"])
        elif etype == EventType.ENTRY_CONFIRMED:
            stage = DrawStage(data["draw_stage"])
            entry = Entry(
                entry_id=event["aggregate_id"],
                tournament_id=data["tournament_id"],
                player_id=data["player_id"],
                status=EntryStatus(data["status"]),
                draw_stage=stage,
                accepted_at=_dt(data["accepted_at"]),
                occupies_slot=data.get("occupies_slot"),
            )
            self.entries[entry.entry_id] = entry
            if entry.status == EntryStatus.ALTERNATE:
                self._alternates[(entry.tournament_id, stage)].append(entry.entry_id)
        elif etype == EventType.ENTRY_WITHDRAWN:
            current = self.entries[event["aggregate_id"]]
            self.entries[event["aggregate_id"]] = replace(current, status=EntryStatus.WITHDRAWN)
        elif etype == EventType.ENTRY_REPLACED:
            candidate = self.entries[event["aggregate_id"]]
            slot = data["slot"]
            self.entries[event["aggregate_id"]] = replace(
                candidate,
                status=EntryStatus.REPLACEMENT,
                draw_stage=DrawStage(data["draw_stage"]),
                occupies_slot=slot,
                replaced_entry_id=data["replaced_entry_id"],
            )
            queue = self._alternates[(candidate.tournament_id, candidate.draw_stage)]
            if event["aggregate_id"] in queue:
                queue.remove(event["aggregate_id"])
        elif etype == EventType.MATCH_SCHEDULED:
            mid = event["aggregate_id"]
            record = MatchRecord(
                match_id=mid,
                tournament_id=data["tournament_id"],
                round_name=data["round_name"],
                state=MatchState.NOT_STARTED,
                court_id=data["court_id"],
                window=TimeWindow(_dt(data["start"]), _dt(data["end"])),
                depends_on=tuple(data["depends_on"]),
                config=_cfg_from_dict(data.get("config")),
                player_ids=tuple(data.get("player_ids", ())),
                requires_equipment=tuple(data.get("requires_equipment", ())),
                schedule_version=data["schedule_version"],
            )
            if mid in self.matches:
                previous = self.matches[mid]
                record.state = previous.state
                record.score = previous.score
                record.winner_id = previous.winner_id
                record.started_at = previous.started_at
                record.completed_at = previous.completed_at
            self.matches[mid] = record
        elif etype == EventType.MATCH_STARTED:
            current = self.matches[event["aggregate_id"]]
            self.matches[event["aggregate_id"]] = replace(current, state=MatchState.ON_COURT, started_at=at)
        elif etype == EventType.MATCH_COMPLETED:
            current = self.matches[event["aggregate_id"]]
            self.matches[event["aggregate_id"]] = replace(
                current,
                state=MatchState.COMPLETED,
                score=data["score"],
                winner_id=data["winner_id"],
                completed_at=_dt(data["at"]),
            )
        elif etype == EventType.PLAYER_MOVEMENT_LOCKED:
            self.movements[event["aggregate_id"]].append(
                {"route": data["route"], "player_ids": tuple(data["player_ids"]), "at": _dt(data["at"])}
            )
        elif etype == EventType.TRAINING_REQUESTED:
            self.training_requests[event["aggregate_id"]] = {
                "tournament_id": data["tournament_id"],
                "court_id": data["court_id"],
                "window": TimeWindow(_dt(data["start"]), _dt(data["end"])),
                "player_ids": tuple(data["player_ids"]),
                "at": _dt(data["at"]),
            }
        elif etype in (EventType.RESOURCE_HELD, EventType.WAITLIST_JOINED):
            item = _commitment_from_dict(data["commitment"])
            self.commitments[item.commitment_id] = item
            if item.waitlist_seq is not None:
                self.wait_seqs[item.resource_id] = max(self.wait_seqs[item.resource_id], item.waitlist_seq)
        elif etype == EventType.DEADLINE_SET:
            current = self.commitments[data["commitment_id"]]
            self.commitments[data["commitment_id"]] = replace(current, deadline=_dt(data["deadline"]))
        elif etype == EventType.RESOURCE_CONFIRMED:
            current = self.commitments[data["commitment_id"]]
            self.commitments[data["commitment_id"]] = replace(
                current, state=CommitmentState.CONFIRMED, confirmed_by=data["by"], confirmed_at=_dt(data["at"])
            )
        elif etype == EventType.WAITLIST_ADVANCED:
            current = self.commitments[data["commitment_id"]]
            self.commitments[data["commitment_id"]] = replace(
                current, state=CommitmentState.PROPOSED, waitlist_seq=current.waitlist_seq
            )
        elif etype == EventType.RESOURCE_RELEASED:
            current = self.commitments[data["commitment_id"]]
            self.commitments[data["commitment_id"]] = replace(
                current, state=CommitmentState.RELEASED, reject_reason=data.get("reason")
            )
        elif etype == EventType.VENUE_CONFIG_PREPARED:
            self.config_changes[event["aggregate_id"]] = {
                "tournament_id": data["tournament_id"],
                "court_id": data["court_id"],
                "from_config": _cfg_from_dict(data["from_config"]),
                "to_config": _cfg_from_dict(data["to_config"]),
                "window": TimeWindow(_dt(data["start"]), _dt(data["end"])),
                "prepared_by": data["prepared_by"],
                "checked_by": None,
                "passed": None,
                "notes": "",
            }
        elif etype == EventType.VENUE_CONFIG_CHECKED:
            change = self.config_changes[event["aggregate_id"]]
            change["checked_by"] = data["checked_by"]
            change["passed"] = data["passed"]
            change["notes"] = data.get("notes", "")
            change["checked_at"] = _dt(data["at"])
        elif etype == EventType.ROOF_EQUIPMENT_STATUS:
            self.equipment[event["aggregate_id"]] = {
                "available": data["available"], "note": data.get("note", ""), "at": _dt(data["at"])
            }
        elif etype == EventType.SHIFT_PUBLISHED:
            self.shifts[event["aggregate_id"]] = {
                "tournament_id": data["tournament_id"],
                "crew_id": data["crew_id"],
                "role": data["role"],
                "at": _dt(data["at"]),
            }
        elif etype == EventType.SECURITY_ZONE_ASSIGNED:
            self.security_zones[event["aggregate_id"]] = {
                "tournament_id": data["tournament_id"],
                "window": TimeWindow(_dt(data["start"]), _dt(data["end"])),
                "note": data["note"],
                "at": _dt(data["at"]),
            }
        elif etype == EventType.VENUE_RELEASED:
            self.venue_releases.append({"court_id": data["court_id"], "by": data["by"], "at": _dt(data["at"])})
