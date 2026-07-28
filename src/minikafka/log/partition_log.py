from __future__ import annotations

from pathlib import Path

from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.batch_codec import encode_batch
from minikafka.core.record import LogRecord
from minikafka.errors import InvalidRecord, OffsetOutOfRange, StorageError
from minikafka.log.segment import LocatedBatch, Segment


class PartitionLog:
    def __init__(
        self,
        directory: Path,
        config: MiniKafkaConfig,
        segments: list[Segment],
        *,
        leader_epoch: int,
    ) -> None:
        self.directory = directory
        self.config = config
        self._segments = segments
        self.leader_epoch = leader_epoch

    @classmethod
    def open(
        cls,
        directory: Path,
        config: MiniKafkaConfig,
        *,
        leader_epoch: int = 0,
    ) -> PartitionLog:
        directory.mkdir(parents=True, exist_ok=True)
        bases: list[int] = []
        for path in directory.glob("*.log"):
            try:
                bases.append(int(path.stem))
            except ValueError as error:
                raise StorageError(f"invalid segment filename {path.name}") from error
        bases.sort()
        if not bases:
            segments = [
                Segment.create(
                    directory,
                    base_offset=0,
                    index_interval_bytes=config.index_interval_bytes,
                )
            ]
        else:
            segments = []
            for index, base in enumerate(bases):
                segment = Segment.open(
                    directory,
                    base,
                    config.index_interval_bytes,
                    active=index == len(bases) - 1,
                )
                if segments and base != segments[-1].leo:
                    for opened in segments:
                        opened.close()
                    segment.close()
                    raise StorageError("segment base offsets are not contiguous")
                segments.append(segment)
            stored_epochs = [
                located.batch.leader_epoch
                for segment in segments
                for located in segment.iter_batches()
            ]
            if stored_epochs:
                leader_epoch = max(leader_epoch, max(stored_epochs))
        return cls(
            directory,
            config,
            segments,
            leader_epoch=leader_epoch,
        )

    @property
    def segments(self) -> tuple[Segment, ...]:
        return tuple(self._segments)

    @property
    def active(self) -> Segment:
        return self._segments[-1]

    @property
    def closed_segments(self) -> tuple[Segment, ...]:
        return tuple(self._segments[:-1])

    @property
    def log_start_offset(self) -> int:
        return self._segments[0].base_offset

    @property
    def leo(self) -> int:
        return self.active.leo

    @property
    def size_bytes(self) -> int:
        return sum(
            segment.size_bytes + segment.index_path.stat().st_size
            for segment in self._segments
        )

    def append(self, batch: RecordBatch) -> LocatedBatch:
        assigned = batch.assign(self.leo, self.leader_epoch)
        return self.append_replica_batch(assigned)

    def append_replica_batch(self, batch: RecordBatch) -> LocatedBatch:
        if batch.base_offset != self.leo:
            raise InvalidRecord(
                f"expected base offset {self.leo}, got {batch.base_offset}"
            )
        encoded_size = len(encode_batch(batch))
        if (
            self.active.has_data
            and self.active.size_bytes + encoded_size
            > self.config.segment_max_bytes
        ):
            self._roll(self.leo)
        location = self.active.append(batch)
        return LocatedBatch(batch, location.position, location.size_bytes)

    def _roll(self, base_offset: int) -> None:
        self.active.flush()
        self._segments.append(
            Segment.create(
                self.directory,
                base_offset,
                self.config.index_interval_bytes,
            )
        )

    def fetch(
        self,
        offset: int,
        max_records: int,
        *,
        end_offset: int | None = None,
    ) -> tuple[LogRecord, ...]:
        if max_records < 0:
            raise ValueError("max_records cannot be negative")
        if offset < self.log_start_offset or offset > self.leo:
            raise OffsetOutOfRange(offset, self.log_start_offset, self.leo)
        if max_records == 0 or offset == self.leo:
            return ()
        visible_end = self.leo if end_offset is None else min(end_offset, self.leo)
        if offset >= visible_end:
            return ()
        records: list[LogRecord] = []
        for segment in self._segments:
            if segment.leo <= offset or segment.base_offset >= visible_end:
                continue
            for record in segment.scan(max(offset, segment.base_offset)):
                if record.offset >= visible_end:
                    return tuple(records)
                records.append(record)
                if len(records) >= max_records:
                    return tuple(records)
        return tuple(records)

    def read_batches(
        self,
        offset: int,
        max_bytes: int,
        *,
        end_offset: int | None = None,
    ) -> tuple[RecordBatch, ...]:
        if offset < self.log_start_offset or offset > self.leo:
            raise OffsetOutOfRange(offset, self.log_start_offset, self.leo)
        if max_bytes <= 0:
            max_bytes = 1
        visible_end = self.leo if end_offset is None else min(end_offset, self.leo)
        batches: list[RecordBatch] = []
        total = 0
        for segment in self._segments:
            if segment.leo <= offset or segment.base_offset >= visible_end:
                continue
            for located in segment.iter_batches(max(offset, segment.base_offset)):
                batch = located.batch
                if batch.base_offset is None or batch.base_offset >= visible_end:
                    return tuple(batches)
                if batches and total + located.size_bytes > max_bytes:
                    return tuple(batches)
                batches.append(batch)
                total += located.size_bytes
        return tuple(batches)

    def all_batches(self) -> tuple[RecordBatch, ...]:
        return tuple(
            located.batch
            for segment in self._segments
            for located in segment.iter_batches()
        )

    def truncate_to(self, next_offset: int) -> None:
        if next_offset < self.log_start_offset or next_offset > self.leo:
            raise OffsetOutOfRange(
                next_offset,
                self.log_start_offset,
                self.leo,
            )
        if next_offset == self.leo:
            return
        batches = self.all_batches()
        boundaries = {self.log_start_offset, self.leo}
        boundaries.update(batch.next_offset for batch in batches)
        if next_offset not in boundaries:
            raise InvalidRecord(
                f"truncate offset {next_offset} is not a batch boundary"
            )
        kept = tuple(batch for batch in batches if batch.next_offset <= next_offset)
        start = self.log_start_offset
        self._remove_all_segment_files()
        base = kept[0].base_offset if kept else next_offset
        if base is None:
            base = start
        self._segments = [
            Segment.create(
                self.directory,
                base,
                self.config.index_interval_bytes,
            )
        ]
        for batch in kept:
            self.append_replica_batch(batch)

    def _remove_all_segment_files(self) -> None:
        paths: list[Path] = []
        for segment in self._segments:
            paths.extend((segment.log_path, segment.index_path))
            segment.close()
        for path in paths:
            path.unlink(missing_ok=True)

    def flush(self) -> None:
        for segment in self._segments:
            segment.flush()

    def close(self) -> None:
        for segment in self._segments:
            segment.close()
