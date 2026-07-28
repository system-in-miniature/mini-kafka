from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from minikafka.core.batch import RecordBatch
from minikafka.core.batch_codec import FRAME_HEADER, decode_batch, encode_batch
from minikafka.core.record import LogRecord
from minikafka.errors import CorruptBatch, InvalidRecord, StorageError
from minikafka.log.index import OffsetIndex


@dataclass(frozen=True, slots=True)
class BatchLocation:
    position: int
    size_bytes: int


@dataclass(frozen=True, slots=True)
class LocatedBatch:
    batch: RecordBatch
    position: int
    size_bytes: int


def segment_stem(base_offset: int) -> str:
    return f"{base_offset:020d}"


def _read_located_batch(file: BinaryIO, position: int) -> LocatedBatch | None:
    file.seek(position)
    header = file.read(FRAME_HEADER.size)
    if not header:
        return None
    if len(header) != FRAME_HEADER.size:
        raise CorruptBatch("batch length is shorter than frame header")
    _, payload_length, _ = FRAME_HEADER.unpack(header)
    payload = file.read(payload_length)
    encoded = header + payload
    batch = decode_batch(encoded)
    return LocatedBatch(batch, position, len(encoded))


class Segment:
    def __init__(
        self,
        directory: Path,
        base_offset: int,
        index_interval_bytes: int,
        log_file: BinaryIO,
        index: OffsetIndex,
        *,
        leo: int,
        max_timestamp_ms: int,
        last_index_position: int,
    ) -> None:
        self.directory = directory
        self.base_offset = base_offset
        self.index_interval_bytes = index_interval_bytes
        self._log = log_file
        self.index = index
        self.leo = leo
        self.max_timestamp_ms = max_timestamp_ms
        self._last_index_position = last_index_position

    @classmethod
    def create(
        cls,
        directory: Path,
        base_offset: int,
        index_interval_bytes: int,
    ) -> Segment:
        if index_interval_bytes <= 0:
            raise ValueError("index_interval_bytes must be positive")
        directory.mkdir(parents=True, exist_ok=True)
        stem = segment_stem(base_offset)
        log_path = directory / f"{stem}.log"
        index_path = directory / f"{stem}.index"
        return cls(
            directory,
            base_offset,
            index_interval_bytes,
            log_path.open("w+b"),
            OffsetIndex.create(index_path, base_offset),
            leo=base_offset,
            max_timestamp_ms=-1,
            last_index_position=-index_interval_bytes,
        )

    @classmethod
    def open(
        cls,
        directory: Path,
        base_offset: int,
        index_interval_bytes: int,
        *,
        active: bool,
    ) -> Segment:
        stem = segment_stem(base_offset)
        log_path = directory / f"{stem}.log"
        if not log_path.exists():
            raise StorageError(f"missing segment log {log_path}")
        log_file = log_path.open("r+b")
        located: list[LocatedBatch] = []
        position = 0
        expected_offset = base_offset
        while True:
            try:
                item = _read_located_batch(log_file, position)
                if item is None:
                    break
                if item.batch.base_offset != expected_offset:
                    raise CorruptBatch(
                        "segment batch offsets are not contiguous"
                    )
                located.append(item)
                expected_offset = item.batch.next_offset
                position += item.size_bytes
            except (CorruptBatch, InvalidRecord) as error:
                if not active:
                    log_file.close()
                    raise StorageError(
                        f"corrupt closed segment {log_path}: {error}"
                    ) from error
                log_file.truncate(position)
                log_file.flush()
                break

        index_path = directory / f"{stem}.index"
        if index_path.exists():
            index_path.unlink()
        index = OffsetIndex.create(index_path, base_offset)
        last_index_position = -index_interval_bytes
        for item in located:
            if (
                not index.entries
                or item.position - last_index_position >= index_interval_bytes
            ):
                index.append(item.batch.base_offset, item.position)
                last_index_position = item.position
        log_file.seek(0, os.SEEK_END)
        timestamps = [
            item.batch.max_timestamp_ms
            for item in located
            if item.batch.max_timestamp_ms >= 0
        ]
        return cls(
            directory,
            base_offset,
            index_interval_bytes,
            log_file,
            index,
            leo=expected_offset,
            max_timestamp_ms=max(timestamps, default=-1),
            last_index_position=last_index_position,
        )

    @property
    def log_path(self) -> Path:
        return self.directory / f"{segment_stem(self.base_offset)}.log"

    @property
    def index_path(self) -> Path:
        return self.directory / f"{segment_stem(self.base_offset)}.index"

    @property
    def size_bytes(self) -> int:
        current = self._log.tell()
        self._log.seek(0, os.SEEK_END)
        size = self._log.tell()
        self._log.seek(current)
        return size

    @property
    def has_data(self) -> bool:
        return self.leo > self.base_offset

    def append(self, batch: RecordBatch) -> BatchLocation:
        if batch.base_offset != self.leo:
            raise InvalidRecord(
                f"expected base offset {self.leo}, got {batch.base_offset}"
            )
        encoded = encode_batch(batch)
        self._log.seek(0, os.SEEK_END)
        position = self._log.tell()
        self._log.write(encoded)
        if (
            not self.index.entries
            or position - self._last_index_position
            >= self.index_interval_bytes
        ):
            self.index.append(batch.base_offset, position)
            self._last_index_position = position
        self.leo = batch.next_offset
        self.max_timestamp_ms = max(
            self.max_timestamp_ms,
            batch.max_timestamp_ms,
        )
        return BatchLocation(position, len(encoded))

    def iter_batches(self, offset: int | None = None) -> Iterator[LocatedBatch]:
        requested = self.base_offset if offset is None else offset
        position = self.index.floor_position(requested)
        while True:
            item = _read_located_batch(self._log, position)
            if item is None:
                return
            if item.batch.next_offset > requested:
                yield item
            position += item.size_bytes

    def scan(self, offset: int) -> tuple[LogRecord, ...]:
        records: list[LogRecord] = []
        for item in self.iter_batches(offset):
            if item.batch.base_offset is None:
                raise StorageError("stored batch has no base offset")
            for delta, record in enumerate(item.batch.records):
                record_offset = item.batch.base_offset + delta
                if record_offset >= offset:
                    records.append(
                        LogRecord(
                            offset=record_offset,
                            key=record.key,
                            value=record.value,
                            timestamp_ms=record.timestamp_ms,
                            headers=record.headers,
                        )
                    )
        return tuple(records)

    def flush(self) -> None:
        self._log.flush()
        os.fsync(self._log.fileno())
        self.index.flush()

    def close(self) -> None:
        self.index.close()
        if not self._log.closed:
            self._log.close()
