"""赛事转换中枢命令层。

所有业务动作都翻译成一个或多个领域事件的原子提交：

* 排程建议只是 PROPOSED 时间承诺，经赛事值班长确认（PROPOSAL_CONFIRMED）才占用资源；
* 确认与资源持有在同一批次提交，资源流版本构成互斥锁，并发确认只有一方成功；
* 冲突建议进入候补队列，释放资源后按"优先级、同优先级按到达先后"推进；
* 降雨/超时只能改尚未开赛的比赛及其未开赛后继链，完赛成绩、球员动线只追加不改写；
* 名单变更按当时签表阶段套用递补规则，重排不得越过资格赛或造成重复占位。
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from .events import ConcurrencyError, Event, EventStore, InMemoryEventStore, parse_ts, utc_now
from .state import (
    ALTERNATE,
    COMPLETED,
    CONFIRMED,
    DIRECT,
    LUCKY_LOSER,
    NOT_STARTED,
    PROPOSED,
    QUALIFIER,
    QUALIFYING,
    STARTED,
    WAITING,
    WILDCARD,
    HubState,
    Slot,
    fold,
)


class DomainError(RuntimeError):
    """业务规则冲突，消息可直接展示给值班长。"""


class ProposalWaitlisted(DomainError):
    """并发确认落败：资源时段被另一方先持有，本提案已进入候补。"""

    def __init__(self, proposal_id: str, resource_id: str) -> None:
        self.proposal_id = proposal_id
        self.resource_id = resource_id
        super().__init__(f"并发确认落败，提案 {proposal_id} 已进入 {resource_id} 候补队列")


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class TurnaroundHub:
    def __init__(self, store: EventStore | None = None) -> None:
        self.store = store or InMemoryEventStore()
        self.state: HubState = fold(self.store.events())

    # ---- 内部提交 ----
    def _commit(self, specs: list[tuple[str, str, str, str, dict]], at: datetime) -> list[Event]:
        """specs: (event_type, aggregate_type, aggregate_id, summary, payload)。

        同时给出每条事件的期望版本，整批原子：任一流版本被他人推进则整批失败。
        """
        entries: list[tuple[Event, int | None]] = []
        next_versions: dict[str, int] = {}
        for event_type, agg_type, agg_id, summary, payload in specs:
            stream = f"{agg_type}:{agg_id}"
            if stream not in next_versions:
                next_versions[stream] = self.store.version(stream)
            next_versions[stream] += 1
            expected = next_versions[stream] - 1
            event = Event(
                event_id=_new_id("evt"),
                event_type=event_type,
                aggregate_type=agg_type,
                aggregate_id=agg_id,
                occurred_at=at.isoformat(),
                version=next_versions[stream],
                summary=summary,
                payload=payload,
            )
            entries.append((event, expected))
        committed = self.store.commit(entries)
        for event in committed:
            self.state.apply(event)
        return committed

    # ---- 赛事与资源 ----
    def register_tournament(self, tournament_id: str, name: str, duty_officers: list[str], at: datetime | None = None) -> None:
        if tournament_id in self.state.tournaments:
            raise DomainError("赛事已登记")
        self._commit(
            [("TOURNAMENT_REGISTERED", "tournament", tournament_id, f"登记赛事 {name}",
              {"name": name, "duty_officers": duty_officers})],
            at or utc_now(),
        )

    def register_resource(self, resource_id: str, kind: str, name: str, config: str, at: datetime | None = None) -> None:
        if resource_id in self.state.resources:
            raise DomainError("资源已登记")
        self._commit(
            [("RESOURCE_REGISTERED", "venue_resource", resource_id, f"登记{kind} {name}",
              {"kind": kind, "name": name, "config": config})],
            at or utc_now(),
        )

    def equipment_down(self, resource_id: str, equipment: str, at: datetime | None = None) -> None:
        self._require_resource(resource_id)
        if equipment in self.state.resources[resource_id].equipment_down:
            raise DomainError(f"{equipment} 已处于停用状态")
        self._commit(
            [("EQUIPMENT_DOWN", "venue_resource", resource_id, f"设备停用：{equipment}", {"equipment": equipment})],
            at or utc_now(),
        )

    def equipment_restored(self, resource_id: str, equipment: str, at: datetime | None = None) -> None:
        self._require_resource(resource_id)
        if equipment not in self.state.resources[resource_id].equipment_down:
            raise DomainError(f"{equipment} 并未停用")
        self._commit(
            [("EQUIPMENT_RESTORED", "venue_resource", resource_id, f"设备恢复：{equipment}", {"equipment": equipment})],
            at or utc_now(),
        )
        # 设备状态曾阻塞候补提案，恢复后尝试推进
        for prop in list(self.state.proposals.values()):
            if prop.status == WAITING and prop.resource_id == resource_id:
                self._advance_waitlist(resource_id, at or utc_now())
                break

    # ---- 名单 ----
    def confirm_entry(self, tournament: str, player_id: str, player_name: str, rank: int,
                      stage: str, at: datetime | None = None) -> Event:
        self._require_tournament(tournament)
        if stage not in (QUALIFYING, DIRECT, WILDCARD, ALTERNATE):
            raise DomainError(f"未知名单阶段：{stage}")
        if any(e.player_id == player_id and e.active for e in self.state.entries.get(tournament, [])):
            raise DomainError(f"球员 {player_id} 已在有效名单中，不得重复占位")
        events = self._commit(
            [("ENTRY_CONFIRMED", "roster", tournament,
              f"{player_name} 进入{self._stage_label(stage)}名单（排名 {rank}）",
              {"player_id": player_id, "player_name": player_name, "rank": rank, "stage": stage})],
            at or utc_now(),
        )
        return events[0]

    def withdraw_player(self, tournament: str, player_id: str, at: datetime | None = None) -> list[Event]:
        """退赛按"当时签表规则"递补：

        * 直接入围阶段未锁定：排名最高的候补按序升入直接名单；
        * 阶段锁定后：留下的幸运失败者席位由最高排名候补填补；
        * 资格赛路径产生的席位不被绕过，留待赛事监督裁定；
        * 任何球员都不会同时占两个槽位。
        """
        entries = [e for e in self.state.entries.get(tournament, []) if e.player_id == player_id and e.active]
        if not entries:
            raise DomainError(f"球员 {player_id} 不在有效名单中")
        at = at or utc_now()
        specs: list[tuple[str, str, str, str, dict]] = [
            ("ENTRY_WITHDRAWN", "roster", tournament, f"球员 {player_id} 退赛", {"player_id": player_id})
        ]
        slot = self.state.player_slot(tournament, player_id)
        if slot is not None:
            specs.append(("SLOT_VACATED", "draw", tournament,
                          f"槽位 {slot.slot_id} 因退赛悬空", {"slot_id": slot.slot_id}))
            locked = self.state.draw_stages.get(tournament, {}).get(slot.stage) == "LOCKED"
            if slot.source in (DIRECT, WILDCARD):
                alt = self._top_alternate(tournament)
                if locked:
                    if alt is not None:
                        specs.append(("ALTERNATE_PROMOTED", "roster", tournament,
                                      f"签表锁定：{alt.player_id} 以幸运失败者身份递补",
                                      {"player_id": alt.player_id, "to_stage": LUCKY_LOSER}))
                        specs.append(("SLOT_FILLED", "draw", tournament,
                                      f"幸运失败者 {alt.player_id} 填入 {slot.slot_id}",
                                      {"slot_id": slot.slot_id, "stage": slot.stage, "round": slot.round_name,
                                       "source": LUCKY_LOSER, "player_id": alt.player_id}))
                else:
                    if alt is not None:
                        specs.append(("ALTERNATE_PROMOTED", "roster", tournament,
                                      f"签表未锁定：{alt.player_id} 按排名升入直接名单",
                                      {"player_id": alt.player_id, "to_stage": DIRECT}))
                        specs.append(("SLOT_FILLED", "draw", tournament,
                                      f"{alt.player_id} 按递补顺序填入 {slot.slot_id}",
                                      {"slot_id": slot.slot_id, "stage": slot.stage, "round": slot.round_name,
                                       "source": DIRECT, "player_id": alt.player_id}))
            # source == QUALIFIER：不绕过资格赛，槽位保持悬空
        return self._commit(specs, at)

    def _top_alternate(self, tournament: str):
        """排名数字最小者优先；已占槽位的候补不得重复占位。"""
        occupied = {s.player_id for s in self.state.slots.get(tournament, []) if s.player_id}
        candidates = [
            e for e in self.state.entries.get(tournament, [])
            if e.active and e.stage == ALTERNATE and e.player_id not in occupied
        ]
        return sorted(candidates, key=lambda e: e.rank)[0] if candidates else None

    # ---- 签表 ----
    def open_stage(self, tournament: str, stage: str, at: datetime | None = None) -> None:
        self._require_tournament(tournament)
        self._commit(
            [("DRAW_STAGE_OPENED", "draw", tournament, f"开启{self._stage_label(stage)}签表阶段", {"stage": stage})],
            at or utc_now(),
        )

    def lock_stage(self, tournament: str, stage: str, at: datetime | None = None) -> None:
        if self.state.draw_stages.get(tournament, {}).get(stage) != "OPEN":
            raise DomainError("只能锁定已开启的签表阶段")
        self._commit(
            [("DRAW_STAGE_LOCKED", "draw", tournament, f"锁定{self._stage_label(stage)}签表阶段", {"stage": stage})],
            at or utc_now(),
        )

    def fill_slot(self, tournament: str, slot_id: str, stage: str, round_name: str,
                  source: str, player_id: str, at: datetime | None = None) -> Event:
        """手工填槽（直接/外卡）；资格赛槽位只能由资格赛比赛结果产生。"""
        if source == QUALIFIER:
            raise DomainError("资格赛槽位只能由资格赛比赛结果填入，不得手工指定")
        if self.state.draw_stages.get(tournament, {}).get(stage) != "OPEN":
            raise DomainError(f"{self._stage_label(stage)}签表阶段未开启或已锁定")
        if self.state.player_slot(tournament, player_id) is not None:
            raise DomainError(f"球员 {player_id} 已占用其他槽位，不得重复占位")
        active = [e for e in self.state.entries.get(tournament, []) if e.player_id == player_id and e.active]
        if not active:
            raise DomainError(f"球员 {player_id} 不在有效名单中")
        existing = self.state._slot(tournament, slot_id)
        if existing is not None and existing.source == QUALIFIER:
            raise DomainError("资格赛槽位只能由资格赛比赛结果填入，不得手工指定")
        if existing is not None and existing.player_id is not None:
            raise DomainError(f"槽位 {slot_id} 已有人选")
        events = self._commit(
            [("SLOT_FILLED", "draw", tournament, f"槽位 {slot_id} 填入 {player_id}（{source}）",
              {"slot_id": slot_id, "stage": stage, "round": round_name, "source": source, "player_id": player_id})],
            at or utc_now(),
        )
        return events[0]

    # ---- 比赛与依赖 ----
    def set_match(self, match_id: str, tournament: str, stage: str, round_name: str,
                  predecessors: list[str] | None = None, requires_equipment: list[str] | None = None,
                  resource_id: str = "", at: datetime | None = None) -> None:
        if match_id in self.state.matches:
            raise DomainError("比赛已登记")
        for pred in predecessors or []:
            if pred not in self.state.matches:
                raise DomainError(f"前序比赛 {pred} 尚未登记")
            if self.state.matches[pred].tournament != tournament:
                raise DomainError("不能跨赛事建立比赛依赖")
        if resource_id:
            self._require_resource(resource_id)
        self._commit(
            [("MATCH_DEPENDENCY_SET", "match", match_id, f"登记{self._stage_label(stage)}{round_name}比赛及依赖",
              {"tournament": tournament, "stage": stage, "round": round_name,
               "predecessors": predecessors or [], "requires_equipment": requires_equipment or [],
               "resource_id": resource_id})],
            at or utc_now(),
        )

    # ---- 时间承诺提案 ----
    def submit_proposal(self, category: str, tournament: str, resource_id: str,
                        start: datetime | str, end: datetime | str, title: str,
                        confirm_by: datetime | str, priority: int = 100,
                        match_id: str | None = None, requires_equipment: list[str] | None = None,
                        at: datetime | None = None) -> str:
        self._require_tournament(tournament)
        self._require_resource(resource_id)
        start, end, confirm_by = parse_ts(start), parse_ts(end), parse_ts(confirm_by)
        if end <= start:
            raise DomainError("结束时间必须晚于开始时间")
        if match_id and match_id not in self.state.matches:
            raise DomainError(f"比赛 {match_id} 尚未登记")
        proposal_id = _new_id("prop")
        at = at or utc_now()
        specs = [
            ("PROPOSAL_SUBMITTED", "proposal", proposal_id, f"提交排程建议：{title}",
             {"category": category, "tournament": tournament, "resource_id": resource_id,
              "start": start.isoformat(), "end": end.isoformat(), "title": title,
              "confirm_by": confirm_by.isoformat(), "priority": priority,
              "match_id": match_id, "requires_equipment": requires_equipment or []}),
        ]
        events = self._commit(specs, at)
        # 与已确认承诺冲突则进入候补；待确认建议之间不互斥，由值班长裁决
        if self.state.overlaps(resource_id, start, end):
            self._join_waitlist(proposal_id, resource_id, at)
        return proposal_id

    def _join_waitlist(self, proposal_id: str, resource_id: str, at: datetime) -> None:
        prop = self.state.proposals[proposal_id]
        queue = self.state.waitlists.get(resource_id, [])
        self._commit(
            [("WAITLIST_JOINED", "proposal", proposal_id,
              f"与已确认安排冲突，进入 {resource_id} 候补队列（优先级 {prop.priority}）",
              {"resource_id": resource_id, "seq": len(queue) + 1})],
            at,
        )

    def confirm_proposal(self, proposal_id: str, officer: str, now: datetime | None = None) -> list[Event]:
        now = now or utc_now()
        prop = self.state.proposals.get(proposal_id)
        if prop is None:
            raise DomainError("提案不存在")
        self._assert_officer(prop.tournament, officer)
        if now > prop.confirm_by:
            # 沿用原截止时间：超时即失效，重启后同样处理
            self._commit(
                [("PROPOSAL_EXPIRED", "proposal", proposal_id,
                  f"超过截止时间 {prop.confirm_by.isoformat()} 未确认，提案失效", {})],
                now,
            )
            raise DomainError("提案已超过确认截止时间，按原截止时间失效")
        if prop.status == WAITING:
            raise DomainError("提案仍在候补队列中，需先被推进")
        if prop.status != PROPOSED:
            raise DomainError(f"当前状态 {prop.status} 不可确认")
        # 与已确认安排冲突直接进入候补
        self._raise_if_scheduled_blockers(prop)

        # 资源流是互斥锁：并发确认同一资源时一方会拿到 ConcurrencyError。
        # 非冲突占用只是推进了版本，重试即可；真正的时段冲突落败后进候补。
        for _attempt in range(20):
            try:
                specs = [
                    ("PROPOSAL_CONFIRMED", "proposal", proposal_id,
                     f"值班长 {officer} 确认时间承诺：{prop.title}", {"officer": officer}),
                    ("RESOURCE_HELD", "venue_resource", prop.resource_id,
                     f"资源 {prop.resource_id} 于 {prop.start.isoformat()}–{prop.end.isoformat()} 被 {proposal_id} 持有",
                     {"proposal_id": proposal_id, "tournament": prop.tournament,
                      "start": prop.start.isoformat(), "end": prop.end.isoformat()}),
                ]
                if prop.match_id:
                    specs.append(("MATCH_SCHEDULED", "match", prop.match_id,
                                  f"比赛 {prop.match_id} 获得确认赛程",
                                  {"proposal_id": proposal_id, "start": prop.start.isoformat(), "end": prop.end.isoformat()}))
                return self._commit(specs, now)
            except ConcurrencyError:
                latest = self.state.proposals.get(proposal_id)
                if latest is None or latest.status != PROPOSED:
                    raise DomainError(f"提案状态已变为 {latest.status if latest else '不存在'}，确认失败")
                clashes = self.state.overlaps(prop.resource_id, prop.start, prop.end)
                if clashes:
                    self._join_waitlist(proposal_id, prop.resource_id, now)
                    raise ProposalWaitlisted(proposal_id, prop.resource_id)
                continue
        raise DomainError("资源争用剧烈，多次重试仍未成功，请稍后再试")

    def _raise_if_scheduled_blockers(self, prop) -> None:
        for text in self._confirmation_blockers(prop):
            if text.startswith("资源时段"):
                self._join_waitlist(prop.proposal_id, prop.resource_id, utc_now())
                raise ProposalWaitlisted(prop.proposal_id, prop.resource_id)
        other = [b for b in self._confirmation_blockers(prop) if not b.startswith("资源时段")]
        if other:
            raise DomainError("；".join(other))

    def _confirmation_blockers(self, prop) -> list[str]:
        blockers = []
        clashes = self.state.overlaps(prop.resource_id, prop.start, prop.end)
        if clashes:
            blockers.append("资源时段与已确认安排冲突：" + "、".join(c.proposal_id for c in clashes))
        resource = self.state.resources.get(prop.resource_id)
        for equipment in prop.requires_equipment:
            if resource and equipment in resource.equipment_down:
                blockers.append(f"必需设备 {equipment} 停用")
        if prop.match_id:
            match = self.state.matches[prop.match_id]
            if match.status == COMPLETED:
                blockers.append("比赛已完赛，成绩不可改写")
            if match.status == STARTED:
                blockers.append("比赛已开赛，不可改期")
        return blockers

    def reject_proposal(self, proposal_id: str, officer: str, reason: str, now: datetime | None = None) -> None:
        now = now or utc_now()
        prop = self.state.proposals.get(proposal_id)
        if prop is None or prop.status != PROPOSED:
            raise DomainError("只有待确认提案可驳回")
        self._assert_officer(prop.tournament, officer)
        self._commit(
            [("PROPOSAL_REJECTED", "proposal", proposal_id, f"值班长驳回：{reason}", {"officer": officer, "reason": reason})],
            now,
        )

    def expire_due(self, now: datetime | None = None) -> list[str]:
        """处理所有超过原截止时间仍待确认的安排；重启重放后截止时间不变。"""
        now = now or utc_now()
        expired = []
        for prop in list(self.state.proposals.values()):
            if prop.status == PROPOSED and now > prop.confirm_by:
                self._commit(
                    [("PROPOSAL_EXPIRED", "proposal", prop.proposal_id,
                      f"超过截止时间 {prop.confirm_by.isoformat()} 未确认，提案失效", {})],
                    now,
                )
                expired.append(prop.proposal_id)
        return expired

    def release_resource(self, proposal_id: str, at: datetime | None = None) -> list[Event]:
        """释放已确认占用，并按可解释顺序推进候补队首。"""
        at = at or utc_now()
        prop = self.state.proposals.get(proposal_id)
        if prop is None:
            raise DomainError("提案不存在")
        if not prop.occupies():
            raise DomainError("该提案当前并未占用资源")
        specs = [
            ("VENUE_RELEASED", "venue_resource", prop.resource_id,
             f"释放 {prop.resource_id}：{prop.title}", {"proposal_id": proposal_id})
        ]
        committed = self._commit(specs, at)
        self._advance_waitlist(prop.resource_id, at)
        return committed

    def _advance_waitlist(self, resource_id: str, at: datetime) -> None:
        for candidate in self.state.ordered_waitlist(resource_id):
            if self.state.overlaps(resource_id, candidate.start, candidate.end):
                continue  # 时段仍被占，队首留队，顺序可解释
            if self._confirmation_blockers(candidate):
                continue  # 设备等阻塞未解除，跳过但不丢弃
            queue_pos = self.state.waitlists[resource_id].index(candidate.proposal_id) + 1
            self._commit(
                [("WAITLIST_ADVANCED", "proposal", candidate.proposal_id,
                  f"资源释放：候补第 {queue_pos} 位（优先级 {candidate.priority}）推进为待确认",
                  {"resource_id": resource_id})],
                at,
            )
            return  # 一次只推进一个可服务者，其余保持可解释顺序

    # ---- 比赛进行与不可改写的事实 ----
    def start_match(self, match_id: str, at: datetime | None = None) -> None:
        match = self.state.matches.get(match_id)
        if match is None:
            raise DomainError("比赛不存在")
        if match.status != NOT_STARTED:
            raise DomainError("比赛已开赛或完赛")
        self._commit([("MATCH_STARTED", "match", match_id, f"比赛 {match_id} 开赛", {})], at or utc_now())

    def complete_match(self, match_id: str, winner_id: str, score: str, at: datetime | None = None) -> list[Event]:
        at = at or utc_now()
        match = self.state.matches.get(match_id)
        if match is None:
            raise DomainError("比赛不存在")
        if match.status == COMPLETED:
            raise DomainError("比赛已有完赛成绩，成绩不可改写")
        if match.status != STARTED:
            raise DomainError("比赛尚未开赛，不能记完成绩")
        specs = [
            ("MATCH_COMPLETED", "match", match_id,
             f"比赛 {match_id} 完赛，胜者 {winner_id}（{score}）",
             {"winner_id": winner_id, "score": score}),
        ]
        # 资格赛完赛：胜者填入等待中的资格赛槽位（这是填入 QUALIFIER 槽的唯一路径）
        if match.stage == QUALIFYING:
            target = self._open_qualifier_slot(match.tournament)
            if target is not None:
                specs.append(("SLOT_FILLED", "draw", match.tournament,
                              f"资格赛胜者 {winner_id} 填入槽位 {target.slot_id}",
                              {"slot_id": target.slot_id, "stage": target.stage, "round": target.round_name,
                               "source": QUALIFIER, "player_id": winner_id, "via_match": match_id}))
        return self._commit(specs, at)

    def _open_qualifier_slot(self, tournament: str) -> Slot | None:
        for slot in self.state.slots.get(tournament, []):
            if slot.source == QUALIFIER and slot.player_id is None:
                return slot
        return None

    def register_qualifier_slot(self, tournament: str, slot_id: str, stage: str, round_name: str,
                                at: datetime | None = None) -> None:
        """登记一个空的、只能由资格赛结果填充的槽位。"""
        if self.state._slot(tournament, slot_id) is not None:
            raise DomainError(f"槽位 {slot_id} 已存在")
        # 空槽不产生 SLOT_FILLED 事件，以内部登记事件表达来源约束
        self._commit(
            [("SLOT_FILLED", "draw", tournament, f"登记待资格赛结果填充的槽位 {slot_id}",
              {"slot_id": slot_id, "stage": stage, "round": round_name,
               "source": QUALIFIER, "player_id": ""})],
            at or utc_now(),
        )

    def record_player_movement(self, player_id: str, player_name: str, from_area: str, to_area: str,
                               at: datetime, note: str = "") -> Event:
        """球员实际动线只追加，任何重排命令都不会改写或删除它。"""
        events = self._commit(
            [("PLAYER_MOVEMENT_RECORDED", "player", player_id,
              f"{player_name} 动线：{from_area} → {to_area}" + (f"（{note}）" if note else ""),
              {"player_name": player_name, "from_area": from_area, "to_area": to_area, "note": note})],
            at,
        )
        return events[0]

    # ---- 降雨/超时重排 ----
    def revise_for_disruption(self, match_id: str, reason: str, new_start: datetime | str,
                              new_resource_id: str | None = None, at: datetime | None = None,
                              confirm_by: datetime | str | None = None) -> list[str]:
        """重排某场未开赛比赛及其未开赛后继链。

        * 已开赛/已完赛的比赛一律不动，成绩与动线保持原样；
        * 旧的已确认承诺被标记 SUPERSEDED 并释放占用，但不删除历史；
        * 每条新承诺仍需值班长确认，不能借重排直接占场；
        * 资格赛相关比赛不能借重排跳过前序。
        """
        at = at or utc_now()
        new_start = parse_ts(new_start)
        match = self.state.matches.get(match_id)
        if match is None:
            raise DomainError("比赛不存在")
        if match.status != NOT_STARTED:
            raise DomainError("降雨或超时只能重排尚未开赛的比赛，已开赛/完赛事实不可改写")
        if reason not in ("RAIN", "OVERRUN"):
            raise DomainError("重排原因必须是 RAIN 或 OVERRUN")

        chain = [match_id] + [m for m in self.state.downstream(match_id)
                              if self.state.matches[m].status == NOT_STARTED]
        new_proposal_ids: list[str] = []
        released_resources: list[str] = []
        cursor = new_start
        for current_id in chain:
            current = self.state.matches[current_id]
            old_prop = self.state.proposals.get(current.scheduled_proposal) if current.scheduled_proposal else None
            specs: list[tuple[str, str, str, str, dict]] = []
            if old_prop is not None and old_prop.occupies():
                specs.append(("VENUE_RELEASED", "venue_resource", old_prop.resource_id,
                              f"重排（{reason}）释放原承诺 {old_prop.proposal_id}",
                              {"proposal_id": old_prop.proposal_id}))
                specs.append(("PROPOSAL_SUPERSEDED", "proposal", old_prop.proposal_id,
                              f"因{self._reason_label(reason)}重排被新承诺取代", {"reason": reason}))
                released_resources.append(old_prop.resource_id)
            elif old_prop is not None and old_prop.status in (PROPOSED, WAITING):
                # 旧建议尚未确认也不占用，重排直接取代，防止两条待确认建议并存
                specs.append(("PROPOSAL_SUPERSEDED", "proposal", old_prop.proposal_id,
                              f"因{self._reason_label(reason)}重排被新承诺取代（原建议未确认）", {"reason": reason}))
            resource_id = new_resource_id or (old_prop.resource_id if old_prop else "")
            if not resource_id:
                raise DomainError(f"比赛 {current_id} 没有可沿用的场地，需显式指定")
            duration = (old_prop.end - old_prop.start) if old_prop else timedelta(hours=1, minutes=30)
            start = cursor
            end = start + duration
            deadline = parse_ts(confirm_by) if confirm_by else at + timedelta(hours=2)
            new_id = _new_id("prop")
            specs.append(("PROPOSAL_SUBMITTED", "proposal", new_id,
                          f"{self._reason_label(reason)}重排建议：{current_id}",
                          {"category": "MATCH", "tournament": current.tournament, "resource_id": resource_id,
                           "start": start.isoformat(), "end": end.isoformat(),
                           "title": f"重排比赛 {current_id}", "confirm_by": deadline.isoformat(),
                           "priority": 50, "match_id": current_id,
                           "requires_equipment": list(current.requires_equipment), "revision_of": old_prop.proposal_id if old_prop else None,
                           "reason": reason}))
            specs.append(("SCHEDULE_REVISED", "match", current_id,
                          f"比赛 {current_id} 因{self._reason_label(reason)}改期，等待值班长确认",
                          {"new_proposal_id": new_id, "reason": reason}))
            self._commit(specs, at)
            new_proposal_ids.append(new_id)
            if self.state.overlaps(resource_id, start, end, exclude=new_id):
                self._join_waitlist(new_id, resource_id, at)
            cursor = end + timedelta(minutes=30)
        for resource_id in dict.fromkeys(released_resources):
            self._advance_waitlist(resource_id, at)
        return new_proposal_ids

    # ---- 场地配置转换与检查 ----
    def record_config_transition(self, resource_id: str, from_config: str, to_config: str,
                                 at: datetime) -> Event:
        self._require_resource(resource_id)
        events = self._commit(
            [("CONFIG_TRANSITION_RECORDED", "venue_resource", resource_id,
              f"场地配置计划：{from_config} → {to_config}@{at.isoformat()}",
              {"from_config": from_config, "to_config": to_config, "at": at.isoformat()})],
            at,
        )
        return events[0]

    def complete_inspection(self, resource_id: str, from_config: str, to_config: str,
                            inspector: str, at: datetime) -> None:
        pending = [t for t in self.state.transitions.get(resource_id, [])
                   if t.from_config == from_config and t.to_config == to_config and t.inspected_by is None]
        if not pending:
            raise DomainError("没有待检查的该配置转换")
        self._commit(
            [("INSPECTION_COMPLETED", "venue_resource", resource_id,
              f"{inspector} 完成 {from_config}→{to_config} 检查",
              {"from_config": from_config, "to_config": to_config, "inspector": inspector, "at": at.isoformat()})],
            at,
        )

    # ---- 辅助 ----
    def _require_tournament(self, tournament: str) -> None:
        if tournament not in self.state.tournaments:
            raise DomainError(f"赛事 {tournament} 尚未登记")

    def _require_resource(self, resource_id: str) -> None:
        if resource_id not in self.state.resources:
            raise DomainError(f"资源 {resource_id} 尚未登记")

    def _assert_officer(self, tournament: str, officer: str) -> None:
        info = self.state.tournaments.get(tournament)
        if info is None or officer not in info["duty_officers"]:
            raise DomainError(f"{officer} 不是该赛事值班长")

    @staticmethod
    def _stage_label(stage: str) -> str:
        return {QUALIFYING: "资格赛", DIRECT: "直接入围", WILDCARD: "外卡",
                ALTERNATE: "候补", LUCKY_LOSER: "幸运失败者"}.get(stage, stage)

    @staticmethod
    def _reason_label(reason: str) -> str:
        return {"RAIN": "降雨", "OVERRUN": "比赛超时"}.get(reason, reason)
