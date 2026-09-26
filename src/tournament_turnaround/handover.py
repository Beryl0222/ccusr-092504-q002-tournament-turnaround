"""交接清单：跨班次/跨赛事交接时回答三个问题。

1. 每块场地何时从哪种配置转为下一种配置、谁完成检查；
2. 哪些比赛仍受前序结果或设备状态阻塞；
3. 还有哪些排程建议待确认、候补按什么顺序等待。

``build_handover`` 只从已重放的中枢状态读取，不产生事件；``render_text``
输出可直接打印的中文清单。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from . import rules
from .hub import TurnaroundHub
from .model import (
    Commitment,
    CommitmentState,
    EntryStatus,
    MatchRecord,
    MatchState,
    ResourceKind,
)


@dataclass(frozen=True)
class ConfigTransitionView:
    court_id: str
    from_name: str
    to_name: str
    start: datetime
    end: datetime
    prepared_by: str
    checked_by: str | None
    passed: bool | None
    notes: str
    commitment_state: str | None


@dataclass(frozen=True)
class BlockedMatchView:
    match_id: str
    tournament_id: str
    round_name: str
    court_id: str | None
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class PendingView:
    commitment: Commitment
    waitlist_seq: int | None


@dataclass(frozen=True)
class Handover:
    generated_at: datetime
    config_transitions: list[ConfigTransitionView]
    blocked_matches: list[BlockedMatchView]
    pending: list[Commitment]
    waitlists: dict[str, list[Commitment]]
    open_withdrawals: list[str]
    venue_releases: list[dict]

    def summary(self) -> dict[str, int]:
        return {
            "config_transitions": len(self.config_transitions),
            "blocked_matches": len(self.blocked_matches),
            "pending": len(self.pending),
            "waiting": sum(len(items) for items in self.waitlists.values()),
            "open_withdrawals": len(self.open_withdrawals),
        }


def _commitment_for_change(hub: TurnaroundHub, court_id: str, purpose_marker: str) -> Commitment | None:
    # 配置转换作业物理占用球场，承诺挂在 COURT 资源上
    key = f"{ResourceKind.COURT.value}:{court_id}"
    candidates = [
        c for c in hub.commitments.values()
        if c.resource_id == key and purpose_marker in c.purpose
    ]
    if not candidates:
        return None
    return sorted(candidates, key=lambda c: c.window.start)[-1]


def build_handover(hub: TurnaroundHub, *, now: datetime | None = None) -> Handover:
    now = now or hub.clock()

    # 1) 场地配置转换时间线
    transitions: list[ConfigTransitionView] = []
    for change_id, change in sorted(hub.config_changes.items(), key=lambda kv: kv[1]["window"].start):
        commitment = _commitment_for_change(hub, change["court_id"], change_id)
        transitions.append(ConfigTransitionView(
            court_id=change["court_id"],
            from_name=change["from_config"].name,
            to_name=change["to_config"].name,
            start=change["window"].start,
            end=change["window"].end,
            prepared_by=change["prepared_by"],
            checked_by=change.get("checked_by"),
            passed=change.get("passed"),
            notes=change.get("notes", ""),
            commitment_state=commitment.state.value if commitment else None,
        ))

    # 2) 仍被阻塞的未开赛比赛（前序结果 / 设备 / 配置检查）
    blocked: list[BlockedMatchView] = []
    for match_id in sorted(hub.matches):
        record = hub.matches[match_id]
        if record.state != MatchState.NOT_STARTED:
            continue
        equipment_ok = all(hub.equipment.get(eq, {}).get("available", False) for eq in record.requires_equipment)
        config_ready = _config_ready_for_court(hub, record)
        reasons = rules.blocked_reasons(
            record, hub.matches,
            equipment_ok=equipment_ok, config_ready=config_ready, now=now,
        )
        if reasons:
            blocked.append(BlockedMatchView(
                match_id=match_id,
                tournament_id=record.tournament_id,
                round_name=record.round_name,
                court_id=record.court_id,
                reasons=tuple(reasons),
            ))

    # 3) 待确认建议与各资源候补队列（顺序即可解释顺序）
    pending = sorted(
        [c for c in hub.commitments.values() if c.state == CommitmentState.PROPOSED],
        key=lambda c: (c.window.start, c.resource_id),
    )
    waitlists: dict[str, list[Commitment]] = {}
    for c in hub.commitments.values():
        if c.state == CommitmentState.WAITING:
            waitlists.setdefault(c.resource_id, [])
    for key in waitlists:
        kind, _, resource_id = key.partition(":")
        waitlists[key] = hub.waiting_commitments(ResourceKind(kind), resource_id)

    # 4) 腾出签位但无人递补的退赛（交接时必须有人继续盯候补）
    open_withdrawals: list[str] = []
    replaced_entries = {
        entry.replaced_entry_id
        for entry in hub.entries.values()
        if entry.replaced_entry_id is not None
    }
    for entry_id, entry in sorted(hub.entries.items()):
        if entry.status == EntryStatus.WITHDRAWN and entry.occupies_slot and entry_id not in replaced_entries:
            open_withdrawals.append(
                f"{entry.tournament_id} {entry.player_id}（原签位 {entry.occupies_slot}）无合格候补"
            )

    return Handover(
        generated_at=now,
        config_transitions=transitions,
        blocked_matches=blocked,
        pending=pending,
        waitlists={k: v for k, v in waitlists.items() if v},
        open_withdrawals=open_withdrawals,
        venue_releases=list(hub.venue_releases),
    )


def _config_ready_for_court(hub: TurnaroundHub, record: MatchRecord) -> bool:
    """比赛开始时场地是否已具备其要求的配置。

    无配置要求视为就绪；否则满足以下任一条件即就绪：
    - 存在已检查通过、转入该配置且完成时间不晚于开赛的转换；
    - 当日最近的一次转换在该场结束之后才开始，且其"来源配置"正是本场
      要求的配置（即本场在拆换前的原始配置中进行）。
    """
    if record.config is None or record.window is None:
        return True
    court_id = record.court_id or ""
    court_changes = sorted(
        (change for change in hub.config_changes.values() if change["court_id"] == court_id),
        key=lambda change: change["window"].start,
    )
    if not court_changes:
        return True
    for change in court_changes:
        checked = change.get("checked_by") and change.get("passed") is True
        if change["to_config"].name == record.config.name and checked and change["window"].end <= record.window.start:
            return True
    first = court_changes[0]
    if first["from_config"].name == record.config.name and first["window"].start >= record.window.end:
        return True
    return False


# ---- 中文渲染 ---------------------------------------------------------------

_STATE_LABELS = {
    "proposed": "待确认",
    "held": "已暂存",
    "waiting": "候补中",
    "confirmed": "已确认",
    "rejected": "已拒绝",
    "released": "已释放",
}


def _fmt_window(start: datetime, end: datetime) -> str:
    if start.tzname() == end.tzname():
        return f"{start:%m-%d %H:%M}–{end:%H:%M}"
    return f"{start.isoformat()} – {end.isoformat()}"


def render_text(handover: Handover) -> str:
    lines: list[str] = []
    lines.append(f"赛事转换交接清单（生成于 {handover.generated_at:%Y-%m-%d %H:%M %Z}）")
    lines.append("=" * 56)

    lines.append("一、场地配置转换时间线")
    if not handover.config_transitions:
        lines.append("  （无）")
    for view in handover.config_transitions:
        if view.checked_by and view.passed:
            check = f"检查人 {view.checked_by}（已通过）"
        elif view.checked_by:
            check = f"检查人 {view.checked_by}（未通过：{view.notes}）"
        else:
            check = "尚未完成检查"
        occupancy = f"，作业承诺：{_STATE_LABELS.get(view.commitment_state, view.commitment_state or '未关联')}"
        lines.append(
            f"  · {view.court_id} {_fmt_window(view.start, view.end)} "
            f"{view.from_name} → {view.to_name}；布置 {view.prepared_by}，{check}{occupancy}"
        )

    lines.append("")
    lines.append("二、仍受阻塞的未开赛比赛")
    if not handover.blocked_matches:
        lines.append("  （无）")
    for view in handover.blocked_matches:
        lines.append(f"  · {view.match_id}（{view.tournament_id} {view.round_name}，场地 {view.court_id or '未定'}）")
        for reason in view.reasons:
            lines.append(f"      - {reason}")

    lines.append("")
    lines.append("三、待值班长确认的排程建议")
    if not handover.pending:
        lines.append("  （无）")
    for item in handover.pending:
        deadline = f"，截止 {item.deadline:%m-%d %H:%M}" if item.deadline else ""
        lines.append(
            f"  · [{item.commitment_id}] {item.tournament_id} {item.purpose} "
            f"{_fmt_window(item.window.start, item.window.end)}（{item.proposed_by} 提出{deadline}）"
        )

    lines.append("")
    lines.append("四、资源候补队列（释放后按此顺序推进）")
    if not handover.waitlists:
        lines.append("  （无）")
    for key, items in sorted(handover.waitlists.items()):
        lines.append(f"  · {key}")
        for item in items:
            lines.append(
                f"      #{item.waitlist_seq} {item.tournament_id} {item.purpose} "
                f"{_fmt_window(item.window.start, item.window.end)}"
            )

    lines.append("")
    lines.append("五、待跟进的空缺席位")
    if not handover.open_withdrawals:
        lines.append("  （无）")
    for text in handover.open_withdrawals:
        lines.append(f"  · {text}")

    if handover.venue_releases:
        lines.append("")
        lines.append("六、已交还场地")
        for release in handover.venue_releases:
            lines.append(f"  · {release['court_id']} 由 {release['by']} 于 {release['at']:%m-%d %H:%M} 交还")

    lines.append("")
    lines.append("汇总：" + "，".join(f"{key} {value}" for key, value in handover.summary().items()))
    return "\n".join(lines)
