"""交接清单投影。

值班团队交接时需要回答四件事：
1. 每块场地何时从哪种配置转为下一种配置、谁完成检查；
2. 哪些时间承诺仍待确认（含原截止时间，重启后口径不变）；
3. 哪些比赛仍受前序结果或设备状态阻塞；
4. 候补队列的可解释顺序。

投影只读取 HubState，纯函数，便于在任何重放点生成快照。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime

from .events import parse_ts
from .state import PROPOSED, WAITING, HubState


@dataclass
class TransitionRow:
    resource_id: str
    resource_name: str
    from_config: str
    to_config: str
    at: str
    inspected_by: str | None
    inspected_at: str | None

    @property
    def ready(self) -> bool:
        return self.inspected_by is not None


@dataclass
class PendingRow:
    proposal_id: str
    category: str
    title: str
    tournament: str
    resource_id: str
    start: str
    end: str
    confirm_by: str
    priority: int
    status: str


@dataclass
class BlockedMatchRow:
    match_id: str
    tournament: str
    stage: str
    round_name: str
    status: str
    blockers: list[str]
    scheduled_proposal: str | None


@dataclass
class WaitlistRow:
    resource_id: str
    order: list[dict] = field(default_factory=list)


@dataclass
class Handover:
    as_of: str
    resources: list[dict]
    config_timeline: list[TransitionRow]
    pending_commitments: list[PendingRow]
    blocked_matches: list[BlockedMatchRow]
    waitlists: list[WaitlistRow]
    player_movements: list[dict]

    def to_dict(self) -> dict:
        return {
            "as_of": self.as_of,
            "resources": self.resources,
            "config_timeline": [asdict(row) for row in self.config_timeline],
            "pending_commitments": [asdict(row) for row in self.pending_commitments],
            "blocked_matches": [asdict(row) for row in self.blocked_matches],
            "waitlists": [{"resource_id": w.resource_id, "order": w.order} for w in self.waitlists],
            "player_movements": self.player_movements,
        }


def build_handover(state: HubState, as_of: datetime | str) -> Handover:
    as_of = parse_ts(as_of)

    timeline: list[TransitionRow] = []
    for resource_id, transitions in state.transitions.items():
        resource = state.resources.get(resource_id)
        for item in transitions:
            timeline.append(
                TransitionRow(
                    resource_id=resource_id,
                    resource_name=resource.name if resource else resource_id,
                    from_config=item.from_config,
                    to_config=item.to_config,
                    at=item.at.isoformat(),
                    inspected_by=item.inspected_by,
                    inspected_at=item.inspected_at.isoformat() if item.inspected_at else None,
                )
            )
    timeline.sort(key=lambda row: (row.at, row.resource_id))

    pending: list[PendingRow] = []
    for prop in state.proposals.values():
        if prop.status in (PROPOSED, WAITING):
            pending.append(
                PendingRow(
                    proposal_id=prop.proposal_id,
                    category=prop.category,
                    title=prop.title,
                    tournament=prop.tournament,
                    resource_id=prop.resource_id,
                    start=prop.start.isoformat(),
                    end=prop.end.isoformat(),
                    confirm_by=prop.confirm_by.isoformat(),
                    priority=prop.priority,
                    status=prop.status,
                )
            )
    pending.sort(key=lambda row: (row.confirm_by, row.priority))

    blocked: list[BlockedMatchRow] = []
    for match_id, match in sorted(state.matches.items()):
        if match.status != "NOT_STARTED":
            continue
        blockers = state.match_blockers(match_id, as_of)
        if blockers:
            blocked.append(
                BlockedMatchRow(
                    match_id=match_id,
                    tournament=match.tournament,
                    stage=match.stage,
                    round_name=match.round_name,
                    status=match.status,
                    blockers=blockers,
                    scheduled_proposal=match.scheduled_proposal,
                )
            )

    waitlists: list[WaitlistRow] = []
    for resource_id in sorted(state.waitlists):
        ordered = state.ordered_waitlist(resource_id)
        if not ordered:
            continue
        waitlists.append(
            WaitlistRow(
                resource_id=resource_id,
                order=[
                    {
                        "proposal_id": p.proposal_id,
                        "title": p.title,
                        "priority": p.priority,
                        "waitlist_seq": p.waitlist_seq,
                        "start": p.start.isoformat(),
                        "end": p.end.isoformat(),
                        "explanation": f"优先级 {p.priority}，同优先级按进入候补先后（序号 {p.waitlist_seq}）",
                    }
                    for p in ordered
                ],
            )
        )

    movements: list[dict] = []
    for player_id, events in sorted(state.player_streams.items()):
        for event in events:
            movements.append(
                {
                    "player_id": player_id,
                    "player_name": event.payload.get("player_name", player_id),
                    "from_area": event.payload.get("from_area"),
                    "to_area": event.payload.get("to_area"),
                    "at": event.occurred_at,
                    "note": event.payload.get("note", ""),
                }
            )
    movements.sort(key=lambda item: item["at"])

    resources = [
        {
            "resource_id": rid,
            "name": r.name,
            "kind": r.kind,
            "current_config": r.config,
            "equipment_down": sorted(r.equipment_down),
        }
        for rid, r in sorted(state.resources.items())
    ]

    return Handover(
        as_of=as_of.isoformat(),
        resources=resources,
        config_timeline=timeline,
        pending_commitments=pending,
        blocked_matches=blocked,
        waitlists=waitlists,
        player_movements=movements,
    )
