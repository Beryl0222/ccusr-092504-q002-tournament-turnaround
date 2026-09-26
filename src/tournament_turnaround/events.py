"""领域事件构造与登记类型。

事件信封遵循 contracts/domain.schema.json；此处的常量集合必须与 schema
中的 enum 保持一致（由 tests/test_contracts.py 交叉校验）。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping
from uuid import uuid4

from .contracts import validate_event


class EventType:
    ENTRY_CONFIRMED = "ENTRY_CONFIRMED"
    ENTRY_WITHDRAWN = "ENTRY_WITHDRAWN"
    ENTRY_REPLACED = "ENTRY_REPLACED"
    DRAW_STAGE_SET = "DRAW_STAGE_SET"
    MATCH_SCHEDULED = "MATCH_SCHEDULED"
    MATCH_STARTED = "MATCH_STARTED"
    MATCH_COMPLETED = "MATCH_COMPLETED"
    MATCH_SCORE_RECORDED = "MATCH_SCORE_RECORDED"
    PLAYER_MOVEMENT_LOCKED = "PLAYER_MOVEMENT_LOCKED"
    TRAINING_REQUESTED = "TRAINING_REQUESTED"
    RESOURCE_REGISTERED = "RESOURCE_REGISTERED"
    RESOURCE_HELD = "RESOURCE_HELD"
    RESOURCE_CONFIRMED = "RESOURCE_CONFIRMED"
    RESOURCE_RELEASED = "RESOURCE_RELEASED"
    WAITLIST_JOINED = "WAITLIST_JOINED"
    WAITLIST_ADVANCED = "WAITLIST_ADVANCED"
    DEADLINE_SET = "DEADLINE_SET"
    TOURNAMENT_REGISTERED = "TOURNAMENT_REGISTERED"
    VENUE_CONFIG_PREPARED = "VENUE_CONFIG_PREPARED"
    VENUE_CONFIG_CHECKED = "VENUE_CONFIG_CHECKED"
    ROOF_EQUIPMENT_STATUS = "ROOF_EQUIPMENT_STATUS"
    SHIFT_PUBLISHED = "SHIFT_PUBLISHED"
    SECURITY_ZONE_ASSIGNED = "SECURITY_ZONE_ASSIGNED"
    VENUE_RELEASED = "VENUE_RELEASED"


EVENT_TYPES = frozenset(
    value for name, value in vars(EventType).items() if not name.startswith("_") and isinstance(value, str)
)


class AggregateType:
    TOURNAMENT = "tournament"
    MATCH = "match"
    VENUE_RESOURCE = "venue_resource"
    TURNAROUND_PLAN = "turnaround_plan"
    ENTRY = "entry"
    DRAW = "draw"
    TRAINING_REQUEST = "training_request"
    CONFIG_CHANGE = "config_change"
    EQUIPMENT = "equipment"
    SHIFT = "shift"
    SECURITY_ZONE = "security_zone"


AGGREGATE_TYPES = frozenset(
    value for name, value in vars(AggregateType).items() if not name.startswith("_") and isinstance(value, str)
)


def make_event(
    *,
    event_type: str,
    aggregate_type: str,
    aggregate_id: str,
    occurred_at: datetime,
    version: int,
    summary: str,
    event_id: str | None = None,
    data: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """构造一个符合契约信封的事件；data 作为业务载荷随信封持久化。"""
    if occurred_at.tzinfo is None:
        raise ValueError("occurred_at 必须带时区")
    payload: dict[str, Any] = {
        "event_id": event_id or uuid4().hex,
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "occurred_at": occurred_at.isoformat(),
        "version": version,
        "summary": summary,
    }
    if data:
        payload["data"] = dict(data)
    envelope_schema: dict[str, Any] = {
        "required": ["event_id", "event_type", "aggregate_type", "aggregate_id", "occurred_at", "version", "summary"],
        "properties": {
            "event_type": {"enum": sorted(EVENT_TYPES)},
            "aggregate_type": {"enum": sorted(AGGREGATE_TYPES)},
        },
    }
    issues = validate_event(payload, envelope_schema)
    if issues:
        raise ValueError(f"事件违反契约: {issues}")
    return payload
