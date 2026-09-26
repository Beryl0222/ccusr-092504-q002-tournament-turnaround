"""事件重放得到的中枢状态。

状态只由领域事件折叠而来，服务重启后重放事件即可恢复全部待确认安排、
截止时间、占用区间与签表事实；任何命令都不在状态之外另存数据。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .events import Event, parse_ts


# 提案状态
PROPOSED = "PROPOSED"
CONFIRMED = "CONFIRMED"
REJECTED = "REJECTED"
EXPIRED = "EXPIRED"
SUPERSEDED = "SUPERSEDED"
WAITING = "WAITING"

# 名单阶段 / 槽位来源
QUALIFYING = "QUALIFYING"
QUALIFIER = "QUALIFIER"
DIRECT = "DIRECT"
WILDCARD = "WILDCARD"
ALTERNATE = "ALTERNATE"
LUCKY_LOSER = "LUCKY_LOSER"

# 比赛状态
NOT_STARTED = "NOT_STARTED"
STARTED = "STARTED"
COMPLETED = "COMPLETED"


@dataclass
class Entry:
    player_id: str
    player_name: str
    rank: int
    stage: str  # QUALIFYING / DIRECT / WILDCARD / ALTERNATE / LUCKY_LOSER
    active: bool = True


@dataclass
class Slot:
    slot_id: str
    stage: str
    round_name: str
    source: str  # DIRECT / WILDCARD / QUALIFIER / LUCKY_LOSER
    player_id: str | None = None
    via_match: str | None = None


@dataclass
class MatchView:
    match_id: str
    tournament: str
    stage: str
    round_name: str
    predecessors: list[str] = field(default_factory=list)
    requires_equipment: list[str] = field(default_factory=list)
    resource_id: str = ""
    status: str = NOT_STARTED
    winner_id: str | None = None
    scheduled_proposal: str | None = None  # 最近一次 SCHEDULE_REVISED 指向的提案（可能仍待确认）
    confirmed_since: datetime | None = None

    @property
    def not_started(self) -> bool:
        return self.status == NOT_STARTED


@dataclass
class Proposal:
    proposal_id: str
    category: str  # MATCH / TRAINING / CONFIG_CHANGE / MAINTENANCE / SHIFT / SECURITY
    tournament: str
    resource_id: str
    start: datetime
    end: datetime
    title: str
    confirm_by: datetime
    priority: int
    submitted_seq: int
    match_id: str | None = None
    requires_equipment: list[str] = field(default_factory=list)
    status: str = PROPOSED
    officer: str | None = None
    released: bool = False
    revision_of: str | None = None
    waitlist_seq: int | None = None
    events: list[Event] = field(default_factory=list)

    def occupies(self) -> bool:
        return self.status == CONFIRMED and not self.released


@dataclass
class Resource:
    resource_id: str
    kind: str
    name: str
    config: str
    equipment_down: set[str] = field(default_factory=set)


@dataclass
class ConfigTransition:
    from_config: str
    to_config: str
    at: datetime
    inspected_by: str | None = None
    inspected_at: datetime | None = None


class HubState:
    def __init__(self) -> None:
        self.tournaments: dict[str, dict] = {}
        self.resources: dict[str, Resource] = {}
        # roster / draw 聚合都以 tournament 为聚合根
        self.entries: dict[str, list[Entry]] = {}
        self.draw_stages: dict[str, dict[str, str]] = {}  # tournament -> stage -> OPEN/LOCKED
        self.slots: dict[str, list[Slot]] = {}
        self.matches: dict[str, MatchView] = {}
        self.proposals: dict[str, Proposal] = {}
        self.waitlists: dict[str, list[str]] = {}  # resource -> proposal ids，队首在前
        self.transitions: dict[str, list[ConfigTransition]] = {}
        self.player_streams: dict[str, list[Event]] = {}  # 球员实际动线，只追加

    # ---- 折叠 ----
    def apply(self, event: Event) -> None:
        p = event.payload
        t = event.event_type
        if t == "TOURNAMENT_REGISTERED":
            self.tournaments[event.aggregate_id] = {
                "name": p.get("name", event.aggregate_id),
                "duty_officers": list(p.get("duty_officers", [])),
            }
        elif t == "RESOURCE_REGISTERED":
            self.resources[event.aggregate_id] = Resource(
                event.aggregate_id, p["kind"], p.get("name", event.aggregate_id), p.get("config", "")
            )
        elif t == "ENTRY_CONFIRMED":
            self.entries.setdefault(event.aggregate_id, []).append(
                Entry(p["player_id"], p.get("player_name", p["player_id"]), p["rank"], p["stage"])
            )
        elif t == "ENTRY_WITHDRAWN":
            for entry in self._active_entries(event.aggregate_id, p["player_id"]):
                entry.active = False
        elif t == "ALTERNATE_PROMOTED":
            for entry in self._active_entries(event.aggregate_id, p["player_id"]):
                entry.stage = p["to_stage"]
        elif t == "DRAW_STAGE_OPENED":
            self.draw_stages.setdefault(event.aggregate_id, {})[p["stage"]] = "OPEN"
        elif t == "DRAW_STAGE_LOCKED":
            self.draw_stages.setdefault(event.aggregate_id, {})[p["stage"]] = "LOCKED"
        elif t == "SLOT_FILLED":
            slot = self._slot(event.aggregate_id, p["slot_id"])
            if slot is None:
                slot = Slot(p["slot_id"], p["stage"], p.get("round", ""), p["source"])
                self.slots.setdefault(event.aggregate_id, []).append(slot)
            slot.player_id = p["player_id"] or None
            slot.source = p["source"]  # source 反映当前占据者的入围途径
            slot.via_match = p.get("via_match")
        elif t == "SLOT_VACATED":
            slot = self._slot(event.aggregate_id, p["slot_id"])
            if slot is not None:
                slot.player_id = None
                slot.via_match = None
        elif t == "MATCH_DEPENDENCY_SET":
            self.matches[event.aggregate_id] = MatchView(
                event.aggregate_id,
                p["tournament"],
                p["stage"],
                p.get("round", ""),
                list(p.get("predecessors", [])),
                list(p.get("requires_equipment", [])),
                p.get("resource_id", ""),
            )
        elif t == "MATCH_SCHEDULED":
            match = self.matches[event.aggregate_id]
            match.scheduled_proposal = p["proposal_id"]
            match.confirmed_since = parse_ts(p["start"])
        elif t == "MATCH_STARTED":
            self.matches[event.aggregate_id].status = STARTED
        elif t == "MATCH_COMPLETED":
            match = self.matches[event.aggregate_id]
            match.status = COMPLETED
            match.winner_id = p["winner_id"]
        elif t == "SCHEDULE_REVISED":
            self.matches[event.aggregate_id].scheduled_proposal = p["new_proposal_id"]
        elif t == "PROPOSAL_SUBMITTED":
            self.proposals[event.aggregate_id] = Proposal(
                proposal_id=event.aggregate_id,
                category=p["category"],
                tournament=p["tournament"],
                resource_id=p["resource_id"],
                start=parse_ts(p["start"]),
                end=parse_ts(p["end"]),
                title=p.get("title", p["category"]),
                confirm_by=parse_ts(p["confirm_by"]),
                priority=int(p.get("priority", 100)),
                submitted_seq=int(p.get("seq", event.seq or 0)),
                match_id=p.get("match_id"),
                requires_equipment=list(p.get("requires_equipment", [])),
                revision_of=p.get("revision_of"),
            )
            self.proposals[event.aggregate_id].events.append(event)
        elif t in ("PROPOSAL_CONFIRMED", "PROPOSAL_REJECTED", "PROPOSAL_EXPIRED", "PROPOSAL_SUPERSEDED"):
            proposal = self.proposals[event.aggregate_id]
            proposal.status = {
                "PROPOSAL_CONFIRMED": CONFIRMED,
                "PROPOSAL_REJECTED": REJECTED,
                "PROPOSAL_EXPIRED": EXPIRED,
                "PROPOSAL_SUPERSEDED": SUPERSEDED,
            }[t]
            if t == "PROPOSAL_CONFIRMED":
                proposal.officer = p.get("officer")
            if t == "PROPOSAL_SUPERSEDED":
                self._drop_waitlist(proposal.resource_id, event.aggregate_id)
            proposal.events.append(event)
        elif t == "VENUE_RELEASED":
            proposal = self.proposals.get(p["proposal_id"])
            if proposal is not None:
                proposal.released = True
            self._drop_waitlist(p.get("resource_id", ""), p["proposal_id"])
        elif t == "WAITLIST_JOINED":
            queue = self.waitlists.setdefault(p["resource_id"], [])
            if event.aggregate_id not in queue:
                queue.append(event.aggregate_id)
            prop = self.proposals[event.aggregate_id]
            prop.status = WAITING
            prop.waitlist_seq = int(p.get("seq", len(queue)))
            prop.events.append(event)
        elif t == "WAITLIST_ADVANCED":
            self._drop_waitlist(p["resource_id"], event.aggregate_id)
            prop = self.proposals[event.aggregate_id]
            prop.status = PROPOSED
            prop.waitlist_seq = None
            prop.events.append(event)
        elif t == "EQUIPMENT_DOWN":
            self.resources[event.aggregate_id].equipment_down.add(p["equipment"])
        elif t == "EQUIPMENT_RESTORED":
            self.resources[event.aggregate_id].equipment_down.discard(p["equipment"])
        elif t == "PLAYER_MOVEMENT_RECORDED":
            self.player_streams.setdefault(event.aggregate_id, []).append(event)
        elif t == "CONFIG_TRANSITION_RECORDED":
            self.transitions.setdefault(event.aggregate_id, []).append(
                ConfigTransition(p["from_config"], p["to_config"], parse_ts(p["at"]))
            )
        elif t == "INSPECTION_COMPLETED":
            for item in reversed(self.transitions.get(event.aggregate_id, [])):
                if item.from_config == p["from_config"] and item.to_config == p["to_config"] and item.inspected_by is None:
                    item.inspected_by = p["inspector"]
                    item.inspected_at = parse_ts(p["at"])
                    break

    def _drop_waitlist(self, resource_id: str, proposal_id: str) -> None:
        queue = self.waitlists.get(resource_id)
        if queue and proposal_id in queue:
            queue.remove(proposal_id)

    def _active_entries(self, tournament: str, player_id: str) -> list[Entry]:
        return [e for e in self.entries.get(tournament, []) if e.player_id == player_id and e.active]

    def _slot(self, tournament: str, slot_id: str) -> Slot | None:
        for slot in self.slots.get(tournament, []):
            if slot.slot_id == slot_id:
                return slot
        return None

    # ---- 查询 ----
    def player_slot(self, tournament: str, player_id: str) -> Slot | None:
        for slot in self.slots.get(tournament, []):
            if slot.player_id == player_id:
                return slot
        return None

    def occupying(self, resource_id: str) -> list[Proposal]:
        return [p for p in self.proposals.values() if p.resource_id == resource_id and p.occupies()]

    def overlaps(self, resource_id: str, start: datetime, end: datetime, exclude: str | None = None) -> list[Proposal]:
        if end <= start:
            raise ValueError("结束时间必须晚于开始时间")
        found = []
        for prop in self.occupying(resource_id):
            if prop.proposal_id == exclude:
                continue
            if prop.start < end and start < prop.end:
                found.append(prop)
        return found

    def waitlist(self, resource_id: str) -> list[Proposal]:
        ids = self.waitlists.get(resource_id, [])
        return [self.proposals[i] for i in ids if i in self.proposals]

    def ordered_waitlist(self, resource_id: str) -> list[Proposal]:
        """可解释顺序：优先级升序（数字小者先），同优先级按进入候补的先后。"""
        return sorted(self.waitlist(resource_id), key=lambda p: (p.priority, p.waitlist_seq or p.submitted_seq))

    def downstream(self, match_id: str) -> list[str]:
        """后继比赛（依赖图下游），拓扑顺序。"""
        result: list[str] = []
        stack = [match_id]
        while stack:
            current = stack.pop()
            for other in self.matches.values():
                if current in other.predecessors and other.match_id not in result and other.match_id != match_id:
                    result.append(other.match_id)
                    stack.append(other.match_id)
        return result

    def match_blockers(self, match_id: str, as_of: datetime) -> list[str]:
        match = self.matches[match_id]
        blockers: list[str] = []
        for pred_id in match.predecessors:
            pred = self.matches.get(pred_id)
            if pred is None or pred.status != COMPLETED:
                state = pred.status if pred else "未登记"
                blockers.append(f"等待前序比赛 {pred_id} 完赛（当前：{state}）")
        resource = self.resources.get(self._match_resource(match_id))
        for equipment in match.requires_equipment:
            if resource and equipment in resource.equipment_down:
                blockers.append(f"设备 {equipment} 处于停用状态")
        proposal = self.proposals.get(match.scheduled_proposal) if match.scheduled_proposal else None
        if proposal is None or not proposal.occupies():
            blockers.append("尚未获得值班长确认的赛程承诺")
        for slot in self.slots.get(match.tournament, []):
            if slot.player_id is None and slot.stage == match.stage and slot.round_name == match.round_name:
                blockers.append(f"签表槽位 {slot.slot_id}（来源 {slot.source}）尚无人选")
        return blockers

    def _match_resource(self, match_id: str) -> str:
        match = self.matches[match_id]
        if match.scheduled_proposal:
            prop = self.proposals.get(match.scheduled_proposal)
            if prop:
                return prop.resource_id
        return match.resource_id


def fold(events: list[Event]) -> HubState:
    state = HubState()
    for event in events:
        state.apply(event)
    return state
