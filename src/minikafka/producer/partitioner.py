from __future__ import annotations

import zlib


class Partitioner:
    def __init__(self) -> None:
        self._sticky: dict[int, int] = {}

    def choose(self, partition_count: int, key: bytes | None) -> int:
        if partition_count <= 0:
            raise ValueError("partition_count must be positive")
        if key is not None:
            return zlib.crc32(key) % partition_count
        return self._sticky.setdefault(partition_count, 0)

    def on_batch_closed(self, partition_count: int, partition: int) -> None:
        if self._sticky.get(partition_count) == partition:
            self._sticky[partition_count] = (partition + 1) % partition_count
