"""纯业务规则：区间冲突、候补顺序、递补资格、依赖链与阻塞判定。

这里不持久化任何状态，便于单独测试每条规则。
"""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime

from .model import (
    Commitment,
    CommitmentState,
    DrawStage,
    Entry,
    EntryStatus,
    MatchRecord,
    MatchState,
    TimeWindow,
)


# ---- 时间窗 ----------------------------------------------------------------

def windows_overlap(first: TimeWindow, second: TimeWindow) -> bool:
    """半开区间 [start, end)：端点相接不算冲突，允许无缝衔接。"""
    return first.start < second.end and second.start < first.end


def active_commitment_states() -> frozenset[CommitmentState]:
    """硬占用资源的承诺状态：已暂存与已确认。

    PROPOSED（排程建议）不硬占用——两个团队可同时提出建议，由确认动作
    仲裁，保证并发确认同一时段时只有一方成功。
    """
    return frozenset({CommitmentState.HELD, CommitmentState.CONFIRMED})


def conflicts_with(
    commitment: Commitment,
    others: list[Commitment],
    *,
    include_proposed: bool = False,
) -> list[Commitment]:
    """返回与 commitment 争抢同一资源且时间重叠的承诺。"""
    blocking_states = active_commitment_states()
    if include_proposed:
        blocking_states = frozenset(
            {CommitmentState.PROPOSED, CommitmentState.HELD, CommitmentState.CONFIRMED}
        )
    result: list[Commitment] = []
    for other in others:
        if other.commitment_id == commitment.commitment_id:
            continue
        if other.resource_id != commitment.resource_id or other.kind != commitment.kind:
            continue
        if other.state not in blocking_states:
            continue
        if windows_overlap(commitment.window, other.window):
            result.append(other)
    return result


# ---- 候补顺序（可解释） ------------------------------------------------------

def waitlist_sort_key(commitment: Commitment, tournament_priority: dict[str, int]) -> tuple:
    """候补推进顺序，每个维度都可向值班长解释。

    1. 窗口开始时间早者优先（转换链上的紧前作业先拿资源）；
    2. 赛事优先级（转换窗口内由运行总监统一登记，数值小者优先）；
    3. 加入候补的序号（先到先得，打破一切平局）。
    """
    assert commitment.waitlist_seq is not None
    return (
        commitment.window.start,
        tournament_priority.get(commitment.tournament_id, 1_000_000),
        commitment.waitlist_seq,
    )


def ordered_waitlist(waiting: list[Commitment], tournament_priority: dict[str, int]) -> list[Commitment]:
    return sorted(waiting, key=lambda item: waitlist_sort_key(item, tournament_priority))


# ---- 名单与递补 -------------------------------------------------------------

def slots_occupied(entries: list[Entry]) -> set[str]:
    return {
        entry.occupies_slot
        for entry in entries
        if entry.occupies_slot and entry.status in (EntryStatus.IN, EntryStatus.REPLACEMENT)
    }


def can_replace(
    *,
    withdrawn: Entry,
    candidate: Entry,
    occupied_slots: set[str],
    current_stage: DrawStage,
) -> tuple[bool, str | None]:
    """判断候选递补是否合法。

    规则（对应需求"按当时签表规则处理"）：
    - 只有候补（alternate）可以递补；
    - 不得越过资格赛：只能递补当前签表阶段的名额——资格赛阶段的退赛由
      资格赛候补递补；正赛阶段的退赛由正赛候补递补，资格赛候补不能借
      递补直接跳过资格赛占正赛签位；
    - 不得重复占位：候选已占有名额（occupies_slot 命中）时拒绝；
    - 不能用已退赛者递补。
    """
    if withdrawn.status != EntryStatus.WITHDRAWN:
        return False, "被递补者必须处于退赛状态"
    if candidate.status == EntryStatus.WITHDRAWN:
        return False, "候选球员已退赛"
    if candidate.status == EntryStatus.IN:
        return False, "候选已在名单中，无需递补"
    if candidate.status != EntryStatus.ALTERNATE:
        return False, f"候选状态 {candidate.status.value} 不可递补"
    if candidate.draw_stage != current_stage:
        return False, (
            f"签表阶段不符：当前 {current_stage.value}，候选来自 "
            f"{candidate.draw_stage.value}，不得越过资格赛"
        )
    if candidate.occupies_slot and candidate.occupies_slot in occupied_slots:
        return False, f"候选已占位 {candidate.occupies_slot}，不得重复占位"
    return True, None


# ---- 比赛依赖 ---------------------------------------------------------------

def topological_ready_order(matches: dict[str, MatchRecord]) -> list[str]:
    """返回按前序依赖排序的比赛 id（Kahn 算法）；成环时报错。"""
    indegree: dict[str, int] = {mid: 0 for mid in matches}
    dependents: dict[str, list[str]] = defaultdict(list)
    for mid, record in matches.items():
        for prerequisite in record.depends_on:
            if prerequisite in matches:
                indegree[mid] += 1
                dependents[prerequisite].append(mid)
    queue = deque(sorted(mid for mid, degree in indegree.items() if degree == 0))
    ordered: list[str] = []
    while queue:
        current = queue.popleft()
        ordered.append(current)
        for dependent in sorted(dependents[current]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                queue.append(dependent)
    if len(ordered) != len(matches):
        cyclic = sorted(mid for mid, degree in indegree.items() if degree > 0)
        raise ValueError(f"比赛依赖存在环：{cyclic}")
    return ordered


def downstream_chain(match_id: str, matches: dict[str, MatchRecord]) -> set[str]:
    """一场比赛重排时，可被连带重排的未开赛下游比赛。"""
    result: set[str] = set()
    frontier = [match_id]
    while frontier:
        current = frontier.pop()
        for mid, record in matches.items():
            if current in record.depends_on and mid not in result:
                result.add(mid)
                frontier.append(mid)
    return result


def reschedulable(roots: list[str], matches: dict[str, MatchRecord]) -> tuple[set[str], list[str]]:
    """降雨/超时时只允许重排"尚未开赛"的依赖链。

    返回 (可重排比赛集合, 被冻结的比赛及原因)。已开赛、已结束的比赛不进入
    集合，其下游若已开赛也被冻结；已结束成绩永不改写。
    """
    allowed: set[str] = set()
    frozen: list[str] = []
    for root in roots:
        chain = downstream_chain(root, matches) | {root}
        for mid in sorted(chain):
            record = matches[mid]
            if record.state == MatchState.COMPLETED:
                frozen.append(mid)  # 已结束：成绩冻结
            elif record.state == MatchState.ON_COURT:
                frozen.append(mid)  # 进行中：球员实际动线不可改写
            else:
                allowed.add(mid)
    return allowed, frozen


def blocked_reasons(
    match: MatchRecord,
    matches: dict[str, MatchRecord],
    *,
    equipment_ok: bool,
    config_ready: bool,
    now: datetime,
) -> list[str]:
    """列出比赛当前仍被阻塞的原因（交接清单用）。"""
    reasons: list[str] = []
    for prerequisite in match.depends_on:
        record = matches.get(prerequisite)
        if record is None:
            reasons.append(f"前序比赛 {prerequisite} 不存在")
        elif record.state != MatchState.COMPLETED:
            reasons.append(f"等待前序结果：{prerequisite}（{record.state.value}）")
    if not equipment_ok:
        reasons.append("屋盖/设备状态不可用")
    if not config_ready:
        reasons.append("场地配置尚未完成检查")
    if match.window is not None and match.state == MatchState.NOT_STARTED and match.window.end < now:
        reasons.append("排程时间窗已过且尚未重排")
    return reasons
