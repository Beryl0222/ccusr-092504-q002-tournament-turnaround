"""连续赛事场地转换中枢：领域契约、事件存储与调度内核。"""

from .contracts import ContractIssue, validate_event
from .handover import Handover, build_handover, render_text
from .hub import (
    AuthorizationError,
    ConflictError,
    HubError,
    NotFoundError,
    RuleViolation,
    TurnaroundHub,
)
from .model import (
    Commitment,
    CommitmentState,
    DrawStage,
    EntryStatus,
    MatchState,
    ResourceKind,
    TimeWindow,
    VenueConfig,
)
from .store import ConcurrentAppendError, JsonlEventStore

__all__ = [
    "ContractIssue",
    "validate_event",
    "TurnaroundHub",
    "JsonlEventStore",
    "ConcurrentAppendError",
    "HubError",
    "NotFoundError",
    "AuthorizationError",
    "ConflictError",
    "RuleViolation",
    "ResourceKind",
    "CommitmentState",
    "DrawStage",
    "EntryStatus",
    "MatchState",
    "TimeWindow",
    "VenueConfig",
    "Commitment",
    "build_handover",
    "render_text",
    "Handover",
]
