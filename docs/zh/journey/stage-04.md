# Stage 04 · 可恢复日志段

### 目标

实现可恢复日志段，并能从可执行失败、运行时状态与关键语句解释其边界。

??? note "交付文件"
    - `src/minikafka/core/record.py`
    - `src/minikafka/errors.py`
    - `src/minikafka/log/recovery.py`
    - `src/minikafka/log/segment.py`
    - `tests/log/test_recovery.py`
    - `tests/log/test_segment.py`

### 当前遇到的问题

进程可能在写 Frame 与更新索引之间停止，留下既不完整又不能安全忽略的磁盘状态。

### 测试契约

#### 先看会坏在哪里

恢复测试追加截断字节与损坏 Frame，并区分可移除的不完整尾部和持久前缀内部的损坏。

??? note "文件差异：tests/log/test_recovery.py"
    ```diff
    diff --git a/tests/log/test_recovery.py b/tests/log/test_recovery.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..78cdf629b53f27a2a1e7daa6e062b7e84c99166b
    --- /dev/null
    +++ b/tests/log/test_recovery.py
    @@ -0,0 +1,87 @@
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.core.batch import RecordBatch
    +from minikafka.core.record import Record
    +from minikafka.errors import StorageError
    +from minikafka.log.segment import Segment
    +
    +
    +def assigned_batch(base_offset: int, values: tuple[bytes, ...]) -> RecordBatch:
    +    return RecordBatch.unassigned(
    +        tuple(
    +            Record(None, value, base_offset + index)
    +            for index, value in enumerate(values)
    +        )
    +    ).assign(base_offset, leader_epoch=0)
    +
    +
    +def test_active_tail_is_truncated_to_last_valid_batch(tmp_path: Path) -> None:
    +    segment = Segment.create(tmp_path, 0, index_interval_bytes=8)
    +    segment.append(assigned_batch(0, (b"safe",)))
    +    valid_size = segment.size_bytes
    +    log_path = segment.log_path
    +    segment.close()
    +    with log_path.open("ab") as file:
    +        file.write(b"MKB1\x00\x00\x00\x20partial")
    +
    +    recovered = Segment.open(
    +        tmp_path,
    +        0,
    +        index_interval_bytes=8,
    +        active=True,
    +    )
    +
    +    assert recovered.size_bytes == valid_size
    +    assert recovered.leo == 1
    +    assert [(record.offset, record.value) for record in recovered.scan(0)] == [
    +        (0, b"safe")
    +    ]
    +    recovered.close()
    +
    +
    +def test_active_crc_failure_is_truncated(tmp_path: Path) -> None:
    +    segment = Segment.create(tmp_path, 0, index_interval_bytes=8)
    +    segment.append(assigned_batch(0, (b"safe",)))
    +    valid_size = segment.size_bytes
    +    segment.append(assigned_batch(1, (b"break",)))
    +    log_path = segment.log_path
    +    segment.close()
    +    data = bytearray(log_path.read_bytes())
    +    data[-1] ^= 0x01
    +    log_path.write_bytes(data)
    +
    +    recovered = Segment.open(tmp_path, 0, 8, active=True)
    +
    +    assert recovered.size_bytes == valid_size
    +    assert recovered.leo == 1
    +    recovered.close()
    +
    +
    +def test_closed_segment_corruption_is_not_silently_repaired(tmp_path: Path) -> None:
    +    segment = Segment.create(tmp_path, 0, index_interval_bytes=8)
    +    segment.append(assigned_batch(0, (b"history",)))
    +    log_path = segment.log_path
    +    segment.close()
    +    data = bytearray(log_path.read_bytes())
    +    data[-1] ^= 0x01
    +    log_path.write_bytes(data)
    +
    +    with pytest.raises(StorageError, match="closed segment"):
    +        Segment.open(tmp_path, 0, 8, active=False)
    +
    +
    +def test_missing_index_is_rebuilt_from_log(tmp_path: Path) -> None:
    +    segment = Segment.create(tmp_path, 0, index_interval_bytes=1)
    +    segment.append(assigned_batch(0, (b"a",)))
    +    segment.append(assigned_batch(1, (b"b",)))
    +    index_path = segment.index_path
    +    segment.close()
    +    index_path.unlink()
    +
    +    recovered = Segment.open(tmp_path, 0, 1, active=True)
    +
    +    assert recovered.index.entries[0] == (0, 0)
    +    assert len(recovered.index.entries) == 2
    +    recovered.close()
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

恢复测试追加截断字节与损坏 Frame，并区分可移除的不完整尾部和持久前缀内部的损坏。

**关键测试语句**

```python
assert recovered.size_bytes == valid_size
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

??? note "文件差异：tests/log/test_segment.py"
    ```diff
    diff --git a/tests/log/test_segment.py b/tests/log/test_segment.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..cec28d78774f037749d3d1b76dc79307462c75ef
    --- /dev/null
    +++ b/tests/log/test_segment.py
    @@ -0,0 +1,54 @@
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.core.batch import RecordBatch
    +from minikafka.core.record import Record
    +from minikafka.errors import InvalidRecord
    +from minikafka.log.segment import Segment
    +
    +
    +def assigned_batch(base_offset: int, values: tuple[bytes, ...]) -> RecordBatch:
    +    return RecordBatch.unassigned(
    +        tuple(Record(None, value, base_offset + index) for index, value in enumerate(values))
    +    ).assign(base_offset, leader_epoch=0)
    +
    +
    +def test_segment_appends_and_scans_from_offset(tmp_path: Path) -> None:
    +    segment = Segment.create(
    +        tmp_path,
    +        base_offset=0,
    +        index_interval_bytes=1,
    +    )
    +    segment.append(assigned_batch(0, (b"a", b"b")))
    +    segment.append(assigned_batch(2, (b"c",)))
    +
    +    records = segment.scan(1)
    +
    +    assert [(record.offset, record.value) for record in records] == [
    +        (1, b"b"),
    +        (2, b"c"),
    +    ]
    +    assert segment.leo == 3
    +    assert segment.max_timestamp_ms == 2
    +    segment.close()
    +
    +
    +def test_segment_requires_contiguous_batches(tmp_path: Path) -> None:
    +    segment = Segment.create(tmp_path, 0, index_interval_bytes=8)
    +
    +    with pytest.raises(InvalidRecord, match="expected base offset 0"):
    +        segment.append(assigned_batch(1, (b"x",)))
    +
    +    segment.close()
    +
    +
    +def test_segment_flushes_log_and_index(tmp_path: Path) -> None:
    +    segment = Segment.create(tmp_path, 0, index_interval_bytes=1)
    +    segment.append(assigned_batch(0, (b"x",)))
    +
    +    segment.flush()
    +
    +    assert segment.log_path.stat().st_size == segment.size_bytes
    +    assert segment.index_path.stat().st_size > 0
    +    segment.close()
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

恢复测试追加截断字节与损坏 Frame，并区分可移除的不完整尾部和持久前缀内部的损坏。

**关键测试语句**

```python
assert recovered.size_bytes == valid_size
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Segment 拥有日志文件与稀疏索引。恢复过程扫描完整 Batch、重建派生索引，并且只截断最后一个不完整 Frame。

### 为什么需要这个机制

进程可能在写 Frame 与更新索引之间停止，留下既不完整又不能安全忽略的磁盘状态。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Append 写入编码 Batch 并周期性建立索引；重开时验证持久前缀并重建 Next Offset 与 Byte Position。

### 机制板块

#### 可恢复日志段机制

Append 写入编码 Batch 并周期性建立索引；重开时验证持久前缀并重建 Next Offset 与 Byte Position。

??? note "文件差异：src/minikafka/core/record.py"
    ```diff
    diff --git a/src/minikafka/core/record.py b/src/minikafka/core/record.py
    index 89c05c89222a0ecc78739226f169b2b6e7783db1..4127441d7ea32cf201893cd7a208ed527d2eafe1 100644
    --- a/src/minikafka/core/record.py
    +++ b/src/minikafka/core/record.py
    @@ -26,3 +26,12 @@ class StoredRecord:
         value: bytes | None
         timestamp_ms: int
         headers: tuple[Header, ...] = ()
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class LogRecord:
    +    offset: int
    +    key: bytes | None
    +    value: bytes | None
    +    timestamp_ms: int
    +    headers: tuple[Header, ...] = ()
    ```

??? note "文件差异：src/minikafka/errors.py"
    ```diff
    diff --git a/src/minikafka/errors.py b/src/minikafka/errors.py
    index e377e7caeebee4b88ea6bb9b1c4fb8a7336f03c7..ce404ed7130bb07f19525a5bc7b592350b81765a 100644
    --- a/src/minikafka/errors.py
    +++ b/src/minikafka/errors.py
    @@ -27,3 +27,7 @@ class CorruptBatch(MiniKafkaError):

     class CorruptIndex(MiniKafkaError):
         code = "CORRUPT_INDEX"
    +
    +
    +class StorageError(MiniKafkaError):
    +    code = "STORAGE_ERROR"
    ```

??? note "文件差异：src/minikafka/log/recovery.py"
    ```diff
    diff --git a/src/minikafka/log/recovery.py b/src/minikafka/log/recovery.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..a354dc3e40bd0a052fa119b6a804981fecb40c68
    --- /dev/null
    +++ b/src/minikafka/log/recovery.py
    @@ -0,0 +1,18 @@
    +from __future__ import annotations
    +
    +from pathlib import Path
    +
    +from minikafka.log.segment import Segment
    +
    +
    +def recover_active_segment(
    +    directory: Path,
    +    base_offset: int,
    +    index_interval_bytes: int,
    +) -> Segment:
    +    return Segment.open(
    +        directory,
    +        base_offset,
    +        index_interval_bytes,
    +        active=True,
    +    )
    ```

??? note "文件差异：src/minikafka/log/segment.py"
    ```diff
    diff --git a/src/minikafka/log/segment.py b/src/minikafka/log/segment.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..e70b3546183be34cd87ade43f2e53cd5eb13ed4a
    --- /dev/null
    +++ b/src/minikafka/log/segment.py
    @@ -0,0 +1,242 @@
    +from __future__ import annotations
    +
    +import os
    +from collections.abc import Iterator
    +from dataclasses import dataclass
    +from pathlib import Path
    +from typing import BinaryIO
    +
    +from minikafka.core.batch import RecordBatch
    +from minikafka.core.batch_codec import FRAME_HEADER, decode_batch, encode_batch
    +from minikafka.core.record import LogRecord
    +from minikafka.errors import CorruptBatch, InvalidRecord, StorageError
    +from minikafka.log.index import OffsetIndex
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class BatchLocation:
    +    position: int
    +    size_bytes: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class LocatedBatch:
    +    batch: RecordBatch
    +    position: int
    +    size_bytes: int
    +
    +
    +def segment_stem(base_offset: int) -> str:
    +    return f"{base_offset:020d}"
    +
    +
    +def _read_located_batch(file: BinaryIO, position: int) -> LocatedBatch | None:
    +    file.seek(position)
    +    header = file.read(FRAME_HEADER.size)
    +    if not header:
    +        return None
    +    if len(header) != FRAME_HEADER.size:
    +        raise CorruptBatch("batch length is shorter than frame header")
    +    _, payload_length, _ = FRAME_HEADER.unpack(header)
    +    payload = file.read(payload_length)
    +    encoded = header + payload
    +    batch = decode_batch(encoded)
    +    return LocatedBatch(batch, position, len(encoded))
    +
    +
    +class Segment:
    +    def __init__(
    +        self,
    +        directory: Path,
    +        base_offset: int,
    +        index_interval_bytes: int,
    +        log_file: BinaryIO,
    +        index: OffsetIndex,
    +        *,
    +        leo: int,
    +        max_timestamp_ms: int,
    +        last_index_position: int,
    +    ) -> None:
    +        self.directory = directory
    +        self.base_offset = base_offset
    +        self.index_interval_bytes = index_interval_bytes
    +        self._log = log_file
    +        self.index = index
    +        self.leo = leo
    +        self.max_timestamp_ms = max_timestamp_ms
    +        self._last_index_position = last_index_position
    +
    +    @classmethod
    +    def create(
    +        cls,
    +        directory: Path,
    +        base_offset: int,
    +        index_interval_bytes: int,
    +    ) -> Segment:
    +        if index_interval_bytes <= 0:
    +            raise ValueError("index_interval_bytes must be positive")
    +        directory.mkdir(parents=True, exist_ok=True)
    +        stem = segment_stem(base_offset)
    +        log_path = directory / f"{stem}.log"
    +        index_path = directory / f"{stem}.index"
    +        return cls(
    +            directory,
    +            base_offset,
    +            index_interval_bytes,
    +            log_path.open("w+b"),
    +            OffsetIndex.create(index_path, base_offset),
    +            leo=base_offset,
    +            max_timestamp_ms=-1,
    +            last_index_position=-index_interval_bytes,
    +        )
    +
    +    @classmethod
    +    def open(
    +        cls,
    +        directory: Path,
    +        base_offset: int,
    +        index_interval_bytes: int,
    +        *,
    +        active: bool,
    +    ) -> Segment:
    +        stem = segment_stem(base_offset)
    +        log_path = directory / f"{stem}.log"
    +        if not log_path.exists():
    +            raise StorageError(f"missing segment log {log_path}")
    +        log_file = log_path.open("r+b")
    +        located: list[LocatedBatch] = []
    +        position = 0
    +        expected_offset = base_offset
    +        while True:
    +            try:
    +                item = _read_located_batch(log_file, position)
    +                if item is None:
    +                    break
    +                if item.batch.base_offset != expected_offset:
    +                    raise CorruptBatch(
    +                        "segment batch offsets are not contiguous"
    +                    )
    +                located.append(item)
    +                expected_offset = item.batch.next_offset
    +                position += item.size_bytes
    +            except (CorruptBatch, InvalidRecord) as error:
    +                if not active:
    +                    log_file.close()
    +                    raise StorageError(
    +                        f"corrupt closed segment {log_path}: {error}"
    +                    ) from error
    +                log_file.truncate(position)
    +                log_file.flush()
    +                break
    +
    +        index_path = directory / f"{stem}.index"
    +        if index_path.exists():
    +            index_path.unlink()
    +        index = OffsetIndex.create(index_path, base_offset)
    +        last_index_position = -index_interval_bytes
    +        for item in located:
    +            if (
    +                not index.entries
    +                or item.position - last_index_position >= index_interval_bytes
    +            ):
    +                index.append(item.batch.base_offset, item.position)
    +                last_index_position = item.position
    +        log_file.seek(0, os.SEEK_END)
    +        timestamps = [
    +            item.batch.max_timestamp_ms
    +            for item in located
    +            if item.batch.max_timestamp_ms >= 0
    +        ]
    +        return cls(
    +            directory,
    +            base_offset,
    +            index_interval_bytes,
    +            log_file,
    +            index,
    +            leo=expected_offset,
    +            max_timestamp_ms=max(timestamps, default=-1),
    +            last_index_position=last_index_position,
    +        )
    +
    +    @property
    +    def log_path(self) -> Path:
    +        return self.directory / f"{segment_stem(self.base_offset)}.log"
    +
    +    @property
    +    def index_path(self) -> Path:
    +        return self.directory / f"{segment_stem(self.base_offset)}.index"
    +
    +    @property
    +    def size_bytes(self) -> int:
    +        current = self._log.tell()
    +        self._log.seek(0, os.SEEK_END)
    +        size = self._log.tell()
    +        self._log.seek(current)
    +        return size
    +
    +    @property
    +    def has_data(self) -> bool:
    +        return self.leo > self.base_offset
    +
    +    def append(self, batch: RecordBatch) -> BatchLocation:
    +        if batch.base_offset != self.leo:
    +            raise InvalidRecord(
    +                f"expected base offset {self.leo}, got {batch.base_offset}"
    +            )
    +        encoded = encode_batch(batch)
    +        self._log.seek(0, os.SEEK_END)
    +        position = self._log.tell()
    +        self._log.write(encoded)
    +        if (
    +            not self.index.entries
    +            or position - self._last_index_position
    +            >= self.index_interval_bytes
    +        ):
    +            self.index.append(batch.base_offset, position)
    +            self._last_index_position = position
    +        self.leo = batch.next_offset
    +        self.max_timestamp_ms = max(
    +            self.max_timestamp_ms,
    +            batch.max_timestamp_ms,
    +        )
    +        return BatchLocation(position, len(encoded))
    +
    +    def iter_batches(self, offset: int | None = None) -> Iterator[LocatedBatch]:
    +        requested = self.base_offset if offset is None else offset
    +        position = self.index.floor_position(requested)
    +        while True:
    +            item = _read_located_batch(self._log, position)
    +            if item is None:
    +                return
    +            if item.batch.next_offset > requested:
    +                yield item
    +            position += item.size_bytes
    +
    +    def scan(self, offset: int) -> tuple[LogRecord, ...]:
    +        records: list[LogRecord] = []
    +        for item in self.iter_batches(offset):
    +            if item.batch.base_offset is None:
    +                raise StorageError("stored batch has no base offset")
    +            for delta, record in enumerate(item.batch.records):
    +                record_offset = item.batch.base_offset + delta
    +                if record_offset >= offset:
    +                    records.append(
    +                        LogRecord(
    +                            offset=record_offset,
    +                            key=record.key,
    +                            value=record.value,
    +                            timestamp_ms=record.timestamp_ms,
    +                            headers=record.headers,
    +                        )
    +                    )
    +        return tuple(records)
    +
    +    def flush(self) -> None:
    +        self._log.flush()
    +        os.fsync(self._log.fileno())
    +        self.index.flush()
    +
    +    def close(self) -> None:
    +        self.index.close()
    +        if not self._log.closed:
    +            self._log.close()
    ```

**是什么，为什么现在需要**

Segment 拥有日志文件与稀疏索引。恢复过程扫描完整 Batch、重建派生索引，并且只截断最后一个不完整 Frame。

**在运行时做什么**

Append 写入编码 Batch 并周期性建立索引；重开时验证持久前缀并重建 Next Offset 与 Byte Position。

**关键语句理解**

只有 Decoder 证明失败始于最后一个不完整 Frame 时，尾部截断才安全；日志中段损坏必须暴露出来。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/04-recoverable-segments/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

只有 Decoder 证明失败始于最后一个不完整 Frame 时，尾部截断才安全；日志中段损坏必须暴露出来。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/02-the-log.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/04-recoverable-segments/stage.patch)
