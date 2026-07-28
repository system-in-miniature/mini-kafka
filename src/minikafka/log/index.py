from __future__ import annotations

import os
import struct
from bisect import bisect_right
from pathlib import Path
from typing import BinaryIO

from minikafka.errors import CorruptIndex

ENTRY = struct.Struct(">IQ")


class OffsetIndex:
    def __init__(
        self,
        path: Path,
        base_offset: int,
        file: BinaryIO,
        entries: list[tuple[int, int]],
    ) -> None:
        self.path = path
        self.base_offset = base_offset
        self._file = file
        self._entries = entries

    @classmethod
    def create(cls, path: Path, base_offset: int) -> OffsetIndex:
        path.parent.mkdir(parents=True, exist_ok=True)
        return cls(path, base_offset, path.open("w+b"), [])

    @classmethod
    def open(cls, path: Path, base_offset: int) -> OffsetIndex:
        file = path.open("r+b")
        data = file.read()
        if len(data) % ENTRY.size:
            file.close()
            raise CorruptIndex(f"partial index entry in {path}")
        entries: list[tuple[int, int]] = []
        previous_relative = -1
        previous_position = -1
        for cursor in range(0, len(data), ENTRY.size):
            relative, position = ENTRY.unpack_from(data, cursor)
            if relative <= previous_relative or position <= previous_position:
                file.close()
                raise CorruptIndex(f"non-monotonic index entries in {path}")
            entries.append((relative, position))
            previous_relative = relative
            previous_position = position
        file.seek(0, os.SEEK_END)
        return cls(path, base_offset, file, entries)

    @property
    def entries(self) -> tuple[tuple[int, int], ...]:
        return tuple(
            (self.base_offset + relative, position)
            for relative, position in self._entries
        )

    def append(self, offset: int, position: int) -> None:
        relative = offset - self.base_offset
        if relative < 0 or relative > 0xFFFFFFFF:
            raise ValueError("relative offset out of range")
        if position < 0:
            raise ValueError("position cannot be negative")
        if self._entries and (
            relative <= self._entries[-1][0]
            or position <= self._entries[-1][1]
        ):
            raise ValueError("index entries must be monotonic")
        self._file.write(ENTRY.pack(relative, position))
        self._entries.append((relative, position))

    def floor_position(self, offset: int) -> int:
        if not self._entries:
            return 0
        relative = offset - self.base_offset
        index = bisect_right(self._entries, (relative, 2**64 - 1)) - 1
        if index < 0:
            return 0
        return self._entries[index][1]

    def truncate_to(self, offset: int) -> None:
        keep = 0
        for relative, _ in self._entries:
            if self.base_offset + relative >= offset:
                break
            keep += 1
        if keep == len(self._entries):
            return
        del self._entries[keep:]
        self._file.truncate(keep * ENTRY.size)
        self._file.seek(0, os.SEEK_END)

    def flush(self) -> None:
        self._file.flush()
        os.fsync(self._file.fileno())

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()
