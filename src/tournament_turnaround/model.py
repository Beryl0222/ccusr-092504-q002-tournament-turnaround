"""领域模型：枚举与值对象。

这些类型描述中枢关心的业务状态，不包含持久化逻辑；状态迁移由 hub 投影
事件完成，纯判定由 rules 模块提供。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class ResourceKind(str, Enum):
    COURT = "court"  # 比赛/训练场地
    CONFIG_ZONE = "config_zone"  # 配置转换作业区（电子司线、空调分区等）
    CHANNEL = "channel"  # 球员通道
    EQUIPMENT = "equipment"  # 屋盖等共享设备
    CREW = "crew"  # 人员班次
    SECURITY = "security"  # 安保分区


class CommitmentState(str, Enum):
    PROPOSED = "proposed"  # 排程建议，尚未经值班长确认（不硬占用）
    HELD = "held"  # 已暂存资源（两阶段占用），等待值班长确认截止
    WAITING = "waiting"  # 与已确认占用冲突，处于候补队列
    CONFIRMED = "confirmed"  # 值班长已确认，正式占用
    REJECTED = "rejected"
    RELEASED = "released"


class DrawStage(str, Enum):
    ENTRY = "entry"  # 报名/候补名单阶段
    QUALIFYING = "qualifying"  # 资格赛
    MAIN_DRAW = "main_draw"  # 正赛
    FINISHED = "finished"


class MatchState(str, Enum):
    NOT_STARTED = "not_started"  # 已排程未开赛——重排只能作用于这一状态
    ON_COURT = "on_court"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class EntryStatus(str, Enum):
    IN = "in"  # 已进入名单（资格赛或正赛，由 draw_stage 标明）
    WITHDRAWN = "withdrawn"
    REPLACEMENT = "replacement"  # 递补进入
    ALTERNATE = "alternate"  # 候补


@dataclass(frozen=True)
class TimeWindow:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("时间窗必须带时区")
        if self.end <= self.start:
            raise ValueError("时间窗结束时间必须晚于开始时间")


@dataclass(frozen=True)
class VenueConfig:
    """一块场地在某一时段采用的配置：电子司线、空调分区、球员通道等。"""

    name: str
    electronic_lines: bool
    hvac_zone: str
    player_channel: str


@dataclass(frozen=True)
class Commitment:
    """对某资源在某时间窗的带版本承诺（排程建议或确认占用）。"""

    commitment_id: str
    tournament_id: str
    kind: ResourceKind
    resource_id: str  # 命名空间后的资源标识，如 "court:C1"
    window: TimeWindow
    state: CommitmentState
    proposed_by: str
    proposed_at: datetime
    plan_id: str | None = None
    confirmed_by: str | None = None
    confirmed_at: datetime | None = None
    deadline: datetime | None = None  # 值班长确认截止；重启后沿用
    waitlist_seq: int | None = None  # 同一资源同一时段的候补序号
    purpose: str = ""
    match_id: str | None = None
    config: VenueConfig | None = None  # 目标配置
    from_config: VenueConfig | None = None  # 配置转换的来源配置
    reject_reason: str | None = None
    replaced_by: str | None = None  # 释放后由哪个候补承诺接管


@dataclass
class Resource:
    resource_id: str
    kind: ResourceKind
    label: str
    commitments: dict[str, Commitment] = field(default_factory=dict)


@dataclass(frozen=True)
class Entry:
    entry_id: str
    tournament_id: str
    player_id: str
    status: EntryStatus
    draw_stage: DrawStage  # 进入名单时所处的签表阶段
    accepted_at: datetime
    replaced_entry_id: str | None = None  # 递补了谁
    occupies_slot: str | None = None  # 占位标识（签位/资格赛名额），防止重复占位


@dataclass
class MatchRecord:
    match_id: str
    tournament_id: str
    round_name: str
    state: MatchState
    court_id: str | None
    window: TimeWindow | None
    depends_on: tuple[str, ...]  # 前序比赛
    config: VenueConfig | None
    player_ids: tuple[str, ...] = ()
    requires_equipment: tuple[str, ...] = ()  # 依赖的设备资源（如屋盖）
    score: str | None = None
    winner_id: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    schedule_version: int = 0  # 每重排一次递增；历史成绩不携带旧版本
