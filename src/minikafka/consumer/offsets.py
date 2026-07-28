from __future__ import annotations

import json
import os
from pathlib import Path

from minikafka.core.metadata import TopicPartition
from minikafka.errors import StorageError


class OffsetStore:
    def __init__(
        self,
        path: Path,
        offsets: dict[str, dict[TopicPartition, int]],
    ) -> None:
        self.path = path
        self._offsets = offsets

    @classmethod
    def open(cls, path: Path) -> OffsetStore:
        if not path.exists():
            return cls(path, {})
        try:
            raw = json.loads(path.read_text())
            offsets = {
                group_id: {
                    TopicPartition(item["topic"], item["partition"]): item["offset"]
                    for item in entries
                }
                for group_id, entries in raw["groups"].items()
            }
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise StorageError(f"invalid offset file {path}") from error
        return cls(path, offsets)

    async def get(self, group_id: str, tp: TopicPartition) -> int | None:
        return self._offsets.get(group_id, {}).get(tp)

    async def commit(
        self,
        group_id: str,
        offsets: dict[TopicPartition, int],
    ) -> None:
        if any(offset < 0 for offset in offsets.values()):
            raise ValueError("committed offsets cannot be negative")
        self._offsets.setdefault(group_id, {}).update(offsets)
        self.flush()

    async def commit_many(
        self,
        groups: dict[str, dict[TopicPartition, int]],
    ) -> None:
        self.commit_many_sync(groups)

    def commit_many_sync(
        self,
        groups: dict[str, dict[TopicPartition, int]],
    ) -> None:
        for group_id, offsets in groups.items():
            if any(offset < 0 for offset in offsets.values()):
                raise ValueError("committed offsets cannot be negative")
            self._offsets.setdefault(group_id, {}).update(offsets)
        self.flush()

    def flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "groups": {
                group_id: [
                    {
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "offset": offset,
                    }
                    for tp, offset in sorted(offsets.items())
                ]
                for group_id, offsets in sorted(self._offsets.items())
            },
        }
        temporary = self.path.with_suffix(".tmp")
        with temporary.open("w") as file:
            json.dump(payload, file, sort_keys=True, separators=(",", ":"))
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, self.path)
        directory_fd = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
