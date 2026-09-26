"""仅追加事件存储：JSONL 持久化 + 按聚合版本的乐观并发。

每个事件的 version 是其所属聚合的单调版本号；append 时校验
``version == 当前版本 + 1``，因此两个并发命令追加同一聚合时只有一方成功。
服务重启时通过 load() 重放全部事件恢复状态。
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Iterable


class ConcurrentAppendError(Exception):
    """聚合版本冲突：并发提交中已有一方先行写入。"""


class JsonlEventStore:
    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._versions: dict[tuple[str, str], int] = {}
        self._loaded = False

    # ---- 读取 ----------------------------------------------------------

    def load(self) -> list[dict[str, Any]]:
        """读取全部历史事件（按写入顺序），并重建聚合版本索引。"""
        with self._lock:
            events: list[dict[str, Any]] = []
            self._versions = {}
            if self.path.exists():
                with self.path.open("r", encoding="utf-8") as handle:
                    for line_no, line in enumerate(handle, start=1):
                        line = line.strip()
                        if not line:
                            continue
                        event = json.loads(line)
                        key = (event["aggregate_type"], event["aggregate_id"])
                        expected = self._versions.get(key, 0) + 1
                        if event["version"] != expected:
                            raise ValueError(
                                f"事件流损坏：{key} 第 {line_no} 行版本 "
                                f"{event['version']} 与期望 {expected} 不符"
                            )
                        self._versions[key] = event["version"]
                        events.append(event)
            self._loaded = True
            return events

    def version_of(self, aggregate_type: str, aggregate_id: str) -> int:
        if not self._loaded:
            self.load()
        return self._versions.get((aggregate_type, aggregate_id), 0)

    # ---- 写入 ----------------------------------------------------------

    def append(self, event: dict[str, Any], *, expected_version: int | None = None) -> None:
        """追加单个事件；expected_version 给出该聚合写入前应有的版本。"""
        self.append_many([event], expected_versions=None if expected_version is None else {
            (event["aggregate_type"], event["aggregate_id"]): expected_version
        })

    def append_many(
        self,
        events: Iterable[dict[str, Any]],
        *,
        expected_versions: dict[tuple[str, str], int] | None = None,
    ) -> None:
        """在一次临界区内原子追加多个事件。

        expected_versions 给出各聚合写入前应有的版本（默认 0 表示新聚合）；
        任何一个聚合版本不匹配则整批拒绝，不写入任何事件。
        """
        events = list(events)
        with self._lock:
            if not self._loaded:
                self.load()
            pending_versions = dict(self._versions)
            expected_versions = expected_versions or {}
            for key, expected in expected_versions.items():
                if pending_versions.get(key, 0) != expected:
                    raise ConcurrentAppendError(
                        f"聚合 {key} 版本冲突：期望 {expected}，实际 {pending_versions.get(key, 0)}"
                    )
            for event in events:
                key = (event["aggregate_type"], event["aggregate_id"])
                next_version = pending_versions.get(key, 0) + 1
                if event["version"] != next_version:
                    raise ConcurrentAppendError(
                        f"聚合 {key} 版本冲突：事件版本 {event['version']}，应为 {next_version}"
                    )
                pending_versions[key] = next_version
            with self.path.open("a", encoding="utf-8") as handle:
                for event in events:
                    handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            self._versions = pending_versions
