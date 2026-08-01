# Stage 05 · 分段 Partition Log

### 目标

实现分段 Partition Log，并能从可执行失败、运行时状态与关键语句解释其边界。

??? note "交付文件"
    - `src/minikafka/config.py`
    - `src/minikafka/log/partition_log.py`
    - `tests/log/test_partition_log.py`
    - `tests/reliability/test_log_restart.py`

### 当前遇到的问题

单个无限增长文件会让 Retention、Recovery 与有界查找难以推理和运维。

### 测试契约

#### 先看会坏在哪里

滚动与重启测试跨越 Segment 边界，并证明进程重开目录后 Offset 与读取仍连续。

??? note "文件差异：tests/log/test_partition_log.py"
    ```diff
    diff --git a/tests/log/test_partition_log.py b/tests/log/test_partition_log.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..fd9a125724b525a58286222925896555c6f41cbb
    --- /dev/null
    +++ b/tests/log/test_partition_log.py
    @@ -0,0 +1,76 @@
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.batch import RecordBatch
    +from minikafka.core.record import Record
    +from minikafka.errors import InvalidRecord, OffsetOutOfRange
    +from minikafka.log.partition_log import PartitionLog
    +
    +
    +def config(tmp_path: Path, *, segment_max_bytes: int = 120) -> MiniKafkaConfig:
    +    return MiniKafkaConfig(
    +        data_dir=tmp_path,
    +        segment_max_bytes=segment_max_bytes,
    +        index_interval_bytes=1,
    +    )
    +
    +
    +def batch(*values: bytes) -> RecordBatch:
    +    return RecordBatch.unassigned(
    +        tuple(Record(None, value, index) for index, value in enumerate(values))
    +    )
    +
    +
    +def test_partition_log_rolls_and_fetches_across_segments(tmp_path: Path) -> None:
    +    log = PartitionLog.open(tmp_path / "events-0", config(tmp_path))
    +    for value in (b"a" * 40, b"b" * 40, b"c" * 40):
    +        log.append(batch(value))
    +
    +    assert len(log.segments) >= 2
    +    assert [record.value for record in log.fetch(1, max_records=10)] == [
    +        b"b" * 40,
    +        b"c" * 40,
    +    ]
    +    assert log.leo == 3
    +    log.close()
    +
    +
    +def test_fetch_enforces_retained_range_and_end_offset(tmp_path: Path) -> None:
    +    log = PartitionLog.open(tmp_path / "events-0", config(tmp_path))
    +    log.append(batch(b"a", b"b", b"c"))
    +
    +    assert [record.offset for record in log.fetch(0, 10, end_offset=2)] == [0, 1]
    +    assert log.fetch(3, 10) == ()
    +    with pytest.raises(OffsetOutOfRange):
    +        log.fetch(4, 1)
    +    log.close()
    +
    +
    +def test_truncate_requires_batch_boundary(tmp_path: Path) -> None:
    +    log = PartitionLog.open(tmp_path / "events-0", config(tmp_path))
    +    log.append(batch(b"a", b"b"))
    +    log.append(batch(b"c"))
    +
    +    with pytest.raises(InvalidRecord, match="batch boundary"):
    +        log.truncate_to(1)
    +
    +    log.truncate_to(2)
    +    assert log.leo == 2
    +    assert [record.value for record in log.fetch(0, 10)] == [b"a", b"b"]
    +    log.close()
    +
    +
    +def test_read_batches_respects_byte_budget_but_returns_one_large_batch(
    +    tmp_path: Path,
    +) -> None:
    +    log = PartitionLog.open(tmp_path / "events-0", config(tmp_path))
    +    log.append(batch(b"a" * 100))
    +    log.append(batch(b"b"))
    +
    +    batches = log.read_batches(0, max_bytes=1)
    +
    +    assert len(batches) == 1
    +    assert batches[0].base_offset == 0
    +    log.close()
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

滚动与重启测试跨越 Segment 边界，并证明进程重开目录后 Offset 与读取仍连续。

**关键测试语句**

```python
assert len(log.segments) >= 2
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

??? note "文件差异：tests/reliability/test_log_restart.py"
    ```diff
    diff --git a/tests/reliability/test_log_restart.py b/tests/reliability/test_log_restart.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..3839a8d2d86af211d5da847be5f8e48bb4750af0
    --- /dev/null
    +++ b/tests/reliability/test_log_restart.py
    @@ -0,0 +1,28 @@
    +from pathlib import Path
    +
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.batch import RecordBatch
    +from minikafka.core.record import Record
    +from minikafka.log.partition_log import PartitionLog
    +
    +
    +def test_restart_preserves_leo_offsets_and_segments(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(
    +        data_dir=tmp_path,
    +        segment_max_bytes=100,
    +        index_interval_bytes=1,
    +    )
    +    directory = tmp_path / "events-0"
    +    log = PartitionLog.open(directory, config)
    +    for value in (b"x" * 40, b"y" * 40, b"z" * 40):
    +        log.append(RecordBatch.unassigned((Record(None, value, 1),)))
    +    bases = tuple(segment.base_offset for segment in log.segments)
    +    log.flush()
    +    log.close()
    +
    +    reopened = PartitionLog.open(directory, config)
    +
    +    assert reopened.leo == 3
    +    assert tuple(segment.base_offset for segment in reopened.segments) == bases
    +    assert [record.offset for record in reopened.fetch(0, 10)] == [0, 1, 2]
    +    reopened.close()
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

滚动与重启测试跨越 Segment 边界，并证明进程重开目录后 Offset 与读取仍连续。

**关键测试语句**

```python
assert len(log.segments) >= 2
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

PartitionLog 是若干不可变 Closed Segment 加一个 Active Segment 的有序序列。Base Offset 定义全局地址空间。

### 为什么需要这个机制

单个无限增长文件会让 Retention、Recovery 与有界查找难以推理和运维。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Append 在超过大小限制前 Roll；Read 选择候选 Segment 再扫描 Batch；重开时排序文件并恢复 Active Tail。

### 机制板块

#### 分段 Partition Log机制

Append 在超过大小限制前 Roll；Read 选择候选 Segment 再扫描 Batch；重开时排序文件并恢复 Active Tail。

??? note "文件差异：src/minikafka/config.py"
    ```diff
    diff --git a/src/minikafka/config.py b/src/minikafka/config.py
    index 57970d3c22428509b0b89b067ee2d5ab4daba27d..f4cad9a0d1f836971a271a3ec56c880c50225a6d 100644
    --- a/src/minikafka/config.py
    +++ b/src/minikafka/config.py
    @@ -8,7 +8,10 @@ from pathlib import Path
     class MiniKafkaConfig:
         data_dir: Path
         segment_max_bytes: int = 1_048_576
    +    index_interval_bytes: int = 4_096

         def __post_init__(self) -> None:
             if self.segment_max_bytes <= 0:
                 raise ValueError("segment_max_bytes must be positive")
    +        if self.index_interval_bytes <= 0:
    +            raise ValueError("index_interval_bytes must be positive")
    ```

??? note "文件差异：src/minikafka/log/partition_log.py"
    ```diff
    diff --git a/src/minikafka/log/partition_log.py b/src/minikafka/log/partition_log.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..e6d7a33c0adeb05f364797901415e2180653fa26
    --- /dev/null
    +++ b/src/minikafka/log/partition_log.py
    @@ -0,0 +1,244 @@
    +from __future__ import annotations
    +
    +from pathlib import Path
    +
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.batch import RecordBatch
    +from minikafka.core.batch_codec import encode_batch
    +from minikafka.core.record import LogRecord
    +from minikafka.errors import InvalidRecord, OffsetOutOfRange, StorageError
    +from minikafka.log.segment import LocatedBatch, Segment
    +
    +
    +class PartitionLog:
    +    def __init__(
    +        self,
    +        directory: Path,
    +        config: MiniKafkaConfig,
    +        segments: list[Segment],
    +        *,
    +        leader_epoch: int,
    +    ) -> None:
    +        self.directory = directory
    +        self.config = config
    +        self._segments = segments
    +        self.leader_epoch = leader_epoch
    +
    +    @classmethod
    +    def open(
    +        cls,
    +        directory: Path,
    +        config: MiniKafkaConfig,
    +        *,
    +        leader_epoch: int = 0,
    +    ) -> PartitionLog:
    +        directory.mkdir(parents=True, exist_ok=True)
    +        bases: list[int] = []
    +        for path in directory.glob("*.log"):
    +            try:
    +                bases.append(int(path.stem))
    +            except ValueError as error:
    +                raise StorageError(f"invalid segment filename {path.name}") from error
    +        bases.sort()
    +        if not bases:
    +            segments = [
    +                Segment.create(
    +                    directory,
    +                    base_offset=0,
    +                    index_interval_bytes=config.index_interval_bytes,
    +                )
    +            ]
    +        else:
    +            segments = []
    +            for index, base in enumerate(bases):
    +                segment = Segment.open(
    +                    directory,
    +                    base,
    +                    config.index_interval_bytes,
    +                    active=index == len(bases) - 1,
    +                )
    +                if segments and base != segments[-1].leo:
    +                    for opened in segments:
    +                        opened.close()
    +                    segment.close()
    +                    raise StorageError("segment base offsets are not contiguous")
    +                segments.append(segment)
    +            stored_epochs = [
    +                located.batch.leader_epoch
    +                for segment in segments
    +                for located in segment.iter_batches()
    +            ]
    +            if stored_epochs:
    +                leader_epoch = max(leader_epoch, max(stored_epochs))
    +        return cls(
    +            directory,
    +            config,
    +            segments,
    +            leader_epoch=leader_epoch,
    +        )
    +
    +    @property
    +    def segments(self) -> tuple[Segment, ...]:
    +        return tuple(self._segments)
    +
    +    @property
    +    def active(self) -> Segment:
    +        return self._segments[-1]
    +
    +    @property
    +    def closed_segments(self) -> tuple[Segment, ...]:
    +        return tuple(self._segments[:-1])
    +
    +    @property
    +    def log_start_offset(self) -> int:
    +        return self._segments[0].base_offset
    +
    +    @property
    +    def leo(self) -> int:
    +        return self.active.leo
    +
    +    @property
    +    def size_bytes(self) -> int:
    +        return sum(
    +            segment.size_bytes + segment.index_path.stat().st_size
    +            for segment in self._segments
    +        )
    +
    +    def append(self, batch: RecordBatch) -> LocatedBatch:
    +        assigned = batch.assign(self.leo, self.leader_epoch)
    +        return self.append_replica_batch(assigned)
    +
    +    def append_replica_batch(self, batch: RecordBatch) -> LocatedBatch:
    +        if batch.base_offset != self.leo:
    +            raise InvalidRecord(
    +                f"expected base offset {self.leo}, got {batch.base_offset}"
    +            )
    +        encoded_size = len(encode_batch(batch))
    +        if (
    +            self.active.has_data
    +            and self.active.size_bytes + encoded_size
    +            > self.config.segment_max_bytes
    +        ):
    +            self._roll(self.leo)
    +        location = self.active.append(batch)
    +        return LocatedBatch(batch, location.position, location.size_bytes)
    +
    +    def _roll(self, base_offset: int) -> None:
    +        self.active.flush()
    +        self._segments.append(
    +            Segment.create(
    +                self.directory,
    +                base_offset,
    +                self.config.index_interval_bytes,
    +            )
    +        )
    +
    +    def fetch(
    +        self,
    +        offset: int,
    +        max_records: int,
    +        *,
    +        end_offset: int | None = None,
    +    ) -> tuple[LogRecord, ...]:
    +        if max_records < 0:
    +            raise ValueError("max_records cannot be negative")
    +        if offset < self.log_start_offset or offset > self.leo:
    +            raise OffsetOutOfRange(offset, self.log_start_offset, self.leo)
    +        if max_records == 0 or offset == self.leo:
    +            return ()
    +        visible_end = self.leo if end_offset is None else min(end_offset, self.leo)
    +        if offset >= visible_end:
    +            return ()
    +        records: list[LogRecord] = []
    +        for segment in self._segments:
    +            if segment.leo <= offset or segment.base_offset >= visible_end:
    +                continue
    +            for record in segment.scan(max(offset, segment.base_offset)):
    +                if record.offset >= visible_end:
    +                    return tuple(records)
    +                records.append(record)
    +                if len(records) >= max_records:
    +                    return tuple(records)
    +        return tuple(records)
    +
    +    def read_batches(
    +        self,
    +        offset: int,
    +        max_bytes: int,
    +        *,
    +        end_offset: int | None = None,
    +    ) -> tuple[RecordBatch, ...]:
    +        if offset < self.log_start_offset or offset > self.leo:
    +            raise OffsetOutOfRange(offset, self.log_start_offset, self.leo)
    +        if max_bytes <= 0:
    +            max_bytes = 1
    +        visible_end = self.leo if end_offset is None else min(end_offset, self.leo)
    +        batches: list[RecordBatch] = []
    +        total = 0
    +        for segment in self._segments:
    +            if segment.leo <= offset or segment.base_offset >= visible_end:
    +                continue
    +            for located in segment.iter_batches(max(offset, segment.base_offset)):
    +                batch = located.batch
    +                if batch.base_offset is None or batch.base_offset >= visible_end:
    +                    return tuple(batches)
    +                if batches and total + located.size_bytes > max_bytes:
    +                    return tuple(batches)
    +                batches.append(batch)
    +                total += located.size_bytes
    +        return tuple(batches)
    +
    +    def all_batches(self) -> tuple[RecordBatch, ...]:
    +        return tuple(
    +            located.batch
    +            for segment in self._segments
    +            for located in segment.iter_batches()
    +        )
    +
    +    def truncate_to(self, next_offset: int) -> None:
    +        if next_offset < self.log_start_offset or next_offset > self.leo:
    +            raise OffsetOutOfRange(
    +                next_offset,
    +                self.log_start_offset,
    +                self.leo,
    +            )
    +        if next_offset == self.leo:
    +            return
    +        batches = self.all_batches()
    +        boundaries = {self.log_start_offset, self.leo}
    +        boundaries.update(batch.next_offset for batch in batches)
    +        if next_offset not in boundaries:
    +            raise InvalidRecord(
    +                f"truncate offset {next_offset} is not a batch boundary"
    +            )
    +        kept = tuple(batch for batch in batches if batch.next_offset <= next_offset)
    +        start = self.log_start_offset
    +        self._remove_all_segment_files()
    +        base = kept[0].base_offset if kept else next_offset
    +        if base is None:
    +            base = start
    +        self._segments = [
    +            Segment.create(
    +                self.directory,
    +                base,
    +                self.config.index_interval_bytes,
    +            )
    +        ]
    +        for batch in kept:
    +            self.append_replica_batch(batch)
    +
    +    def _remove_all_segment_files(self) -> None:
    +        paths: list[Path] = []
    +        for segment in self._segments:
    +            paths.extend((segment.log_path, segment.index_path))
    +            segment.close()
    +        for path in paths:
    +            path.unlink(missing_ok=True)
    +
    +    def flush(self) -> None:
    +        for segment in self._segments:
    +            segment.flush()
    +
    +    def close(self) -> None:
    +        for segment in self._segments:
    +            segment.close()
    ```

**是什么，为什么现在需要**

PartitionLog 是若干不可变 Closed Segment 加一个 Active Segment 的有序序列。Base Offset 定义全局地址空间。

**在运行时做什么**

Append 在超过大小限制前 Roll；Read 选择候选 Segment 再扫描 Batch；重开时排序文件并恢复 Active Tail。

**关键语句理解**

在 Append 前滚动，使每个完成 Segment 保持在配置边界内，同时维持单调的 Partition Offset 序列。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/05-partition-log/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

在 Append 前滚动，使每个完成 Segment 保持在配置边界内，同时维持单调的 Partition Offset 序列。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/02-the-log.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/05-partition-log/stage.patch)
