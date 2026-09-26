"""领域事件与事件存储。

每个聚合一条流，流内版本严格单调递增；跨流的一批事件在一次提交内原子完成，
expected_version 用于乐观并发：两个值班长同时确认同一时段时，只有一次提交能成功。
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Protocol


def parse_ts(value: str | datetime) -> datetime:
    """把 ISO8601 字符串解析为带时区时间，已是 datetime 则原样返回。"""
    if isinstance(value, datetime):
        return value
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("时间必须包含时区")
    return parsed


def to_ts(value: datetime) -> str:
    return value.isoformat()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    occurred_at: str
    version: int
    summary: str
    payload: dict = field(default_factory=dict)
    seq: int = 0  # 全局提交顺序，由存储赋值

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict) -> "Event":
        return cls(**raw)


class ConcurrencyError(RuntimeError):
    """乐观并发冲突：流版本已被其他提交推进。"""

    def __init__(self, stream: str, expected: int | None, current: int) -> None:
        self.stream = stream
        self.expected = expected
        self.current = current
        super().__init__(
            f"流 {stream} 版本冲突：期望 {expected}，当前 {current}（并发确认只有一方成功）"
        )


class EventStore(Protocol):
    def commit(self, entries: Iterable[tuple[Event, int | None]]) -> list[Event]: ...

    def events(self) -> list[Event]: ...

    def version(self, stream: str) -> int: ...


def _stream_of(event: Event) -> str:
    return f"{event.aggregate_type}:{event.aggregate_id}"


class InMemoryEventStore:
    """线程安全的内存事件存储；一次 commit 原子写入多条流。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._events: list[Event] = []
        self._versions: dict[str, int] = {}

    def commit(self, entries: Iterable[tuple[Event, int | None]]) -> list[Event]:
        entries = list(entries)
        if not entries:
            return []
        with self._lock:
            # 先校验整批，任何一条冲突则整批不落地；同一流可在批内携带多条连续版本
            seen_versions: dict[str, int] = {}
            for event, expected in entries:
                stream = _stream_of(event)
                if stream not in seen_versions:
                    current = self._versions.get(stream, 0)
                    if expected is not None and expected != current:
                        raise ConcurrencyError(stream, expected, current)
                    seen_versions[stream] = current
                seen_versions[stream] += 1
                if event.version != seen_versions[stream]:
                    raise ConcurrencyError(stream, seen_versions[stream] - 1,
                                           self._versions.get(stream, 0))
            committed: list[Event] = []
            for event, _expected in entries:
                stream = _stream_of(event)
                self._versions[stream] = self._versions.get(stream, 0) + 1
                seq = len(self._events) + 1
                stored = Event(**{**asdict(event), "seq": seq})
                self._events.append(stored)
                committed.append(stored)
            return committed

    def events(self) -> list[Event]:
        with self._lock:
            return list(self._events)

    def version(self, stream: str) -> int:
        with self._lock:
            return self._versions.get(stream, 0)


class JsonlEventStore(InMemoryEventStore):
    """每行一次提交（一个事件批），重启时重放；截止时间等全部来自事件，不另存状态。"""

    def __init__(self, path: str | Path) -> None:
        super().__init__()
        self.path = Path(path)
        self._file_lock = threading.RLock()
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    records = json.loads(line)
                    events = [Event.from_dict(item) for item in records]
                    # 重放走父类的底层写入，不再重复落盘
                    for event in events:
                        stream = _stream_of(event)
                        current = self._versions.get(stream, 0)
                        if event.version != current + 1:
                            raise ConcurrencyError(stream, current, current)
                        self._versions[stream] = current + 1
                        self._events.append(event)

    def commit(self, entries: Iterable[tuple[Event, int | None]]) -> list[Event]:
        with self._file_lock:
            committed = super().commit(entries)
            if committed:
                payload = [event.to_dict() for event in committed]
                with self.path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
                    fh.flush()
                    os.fsync(fh.fileno())
            return committed
