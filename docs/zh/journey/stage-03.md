# Stage 03 · 稀疏 Offset 查找

### 目标

实现稀疏 Offset 查找，并能从可执行失败、运行时状态与关键语句解释其边界。

??? note "交付文件"
    - `src/minikafka/errors.py`
    - `src/minikafka/log/__init__.py`
    - `src/minikafka/log/index.py`
    - `tests/unit/test_offset_index.py`

### 当前遇到的问题

每次按 Offset 读取都从 Segment 开头扫描，会把随机读取退化为线性工作。

### 测试契约

#### 先看会坏在哪里

Floor Lookup 测试查询两个条目之间的 Offset；损坏条目测试暴露会悄悄返回目标之后位置的索引。

??? note "文件差异：tests/unit/test_offset_index.py"
    ```diff
    diff --git a/tests/unit/test_offset_index.py b/tests/unit/test_offset_index.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..a54c495ce9a4a0d5eefa95d5bb98b6c4084108a9
    --- /dev/null
    +++ b/tests/unit/test_offset_index.py
    @@ -0,0 +1,61 @@
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.errors import CorruptIndex
    +from minikafka.log.index import OffsetIndex
    +
    +
    +def test_sparse_index_returns_floor_position(tmp_path: Path) -> None:
    +    path = tmp_path / "000.index"
    +    index = OffsetIndex.create(path, base_offset=100)
    +    index.append(offset=100, position=0)
    +    index.append(offset=110, position=512)
    +    index.append(offset=125, position=900)
    +
    +    assert index.floor_position(99) == 0
    +    assert index.floor_position(100) == 0
    +    assert index.floor_position(117) == 512
    +    assert index.floor_position(125) == 900
    +
    +    index.close()
    +    reopened = OffsetIndex.open(path, base_offset=100)
    +    assert reopened.floor_position(125) == 900
    +    reopened.close()
    +
    +
    +def test_index_rejects_non_monotonic_entries(tmp_path: Path) -> None:
    +    index = OffsetIndex.create(tmp_path / "000.index", base_offset=0)
    +    index.append(offset=5, position=20)
    +
    +    with pytest.raises(ValueError, match="monotonic"):
    +        index.append(offset=4, position=30)
    +
    +    with pytest.raises(ValueError, match="monotonic"):
    +        index.append(offset=6, position=20)
    +
    +    index.close()
    +
    +
    +def test_index_rejects_partial_entry(tmp_path: Path) -> None:
    +    path = tmp_path / "000.index"
    +    path.write_bytes(b"\x00\x00\x00")
    +
    +    with pytest.raises(CorruptIndex, match="partial"):
    +        OffsetIndex.open(path, base_offset=0)
    +
    +
    +def test_index_can_be_truncated_and_flushed(tmp_path: Path) -> None:
    +    path = tmp_path / "000.index"
    +    index = OffsetIndex.create(path, base_offset=0)
    +    index.append(0, 0)
    +    index.append(10, 100)
    +    index.append(20, 200)
    +
    +    index.truncate_to(offset=15)
    +    index.flush()
    +    index.close()
    +
    +    reopened = OffsetIndex.open(path, base_offset=0)
    +    assert reopened.entries == ((0, 0), (10, 100))
    +    reopened.close()
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

Floor Lookup 测试查询两个条目之间的 Offset；损坏条目测试暴露会悄悄返回目标之后位置的索引。

**关键测试语句**

```python
assert index.floor_position(99) == 0
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

稀疏索引把部分相对 Offset 映射到字节位置。Floor Lookup 返回不大于目标的最近位置，日志再从那里向前扫描。

### 为什么需要这个机制

每次按 Offset 读取都从 Segment 开头扫描，会把随机读取退化为线性工作。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Append 保证条目单调，Lookup 执行有序 Floor Search，Truncate 让索引与缩短后的日志保持一致。

### 机制板块

#### 稀疏 Offset 查找机制

Append 保证条目单调，Lookup 执行有序 Floor Search，Truncate 让索引与缩短后的日志保持一致。

??? note "文件差异：src/minikafka/errors.py"
    ```diff
    diff --git a/src/minikafka/errors.py b/src/minikafka/errors.py
    index 0f6804c028768cd9afa88d49f1b4d40f8f5985e7..e377e7caeebee4b88ea6bb9b1c4fb8a7336f03c7 100644
    --- a/src/minikafka/errors.py
    +++ b/src/minikafka/errors.py
    @@ -23,3 +23,7 @@ class InvalidRecord(MiniKafkaError):

     class CorruptBatch(MiniKafkaError):
         code = "CORRUPT_BATCH"
    +
    +
    +class CorruptIndex(MiniKafkaError):
    +    code = "CORRUPT_INDEX"
    ```

??? note "文件差异：src/minikafka/log/index.py"
    ```diff
    diff --git a/src/minikafka/log/index.py b/src/minikafka/log/index.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..0cc6ccbd9f5fd3129d6aa0c177ffc1cefc3f3235
    --- /dev/null
    +++ b/src/minikafka/log/index.py
    @@ -0,0 +1,101 @@
    +from __future__ import annotations
    +
    +import os
    +import struct
    +from bisect import bisect_right
    +from pathlib import Path
    +from typing import BinaryIO
    +
    +from minikafka.errors import CorruptIndex
    +
    +ENTRY = struct.Struct(">IQ")
    +
    +
    +class OffsetIndex:
    +    def __init__(
    +        self,
    +        path: Path,
    +        base_offset: int,
    +        file: BinaryIO,
    +        entries: list[tuple[int, int]],
    +    ) -> None:
    +        self.path = path
    +        self.base_offset = base_offset
    +        self._file = file
    +        self._entries = entries
    +
    +    @classmethod
    +    def create(cls, path: Path, base_offset: int) -> OffsetIndex:
    +        path.parent.mkdir(parents=True, exist_ok=True)
    +        return cls(path, base_offset, path.open("w+b"), [])
    +
    +    @classmethod
    +    def open(cls, path: Path, base_offset: int) -> OffsetIndex:
    +        file = path.open("r+b")
    +        data = file.read()
    +        if len(data) % ENTRY.size:
    +            file.close()
    +            raise CorruptIndex(f"partial index entry in {path}")
    +        entries: list[tuple[int, int]] = []
    +        previous_relative = -1
    +        previous_position = -1
    +        for cursor in range(0, len(data), ENTRY.size):
    +            relative, position = ENTRY.unpack_from(data, cursor)
    +            if relative <= previous_relative or position <= previous_position:
    +                file.close()
    +                raise CorruptIndex(f"non-monotonic index entries in {path}")
    +            entries.append((relative, position))
    +            previous_relative = relative
    +            previous_position = position
    +        file.seek(0, os.SEEK_END)
    +        return cls(path, base_offset, file, entries)
    +
    +    @property
    +    def entries(self) -> tuple[tuple[int, int], ...]:
    +        return tuple(
    +            (self.base_offset + relative, position)
    +            for relative, position in self._entries
    +        )
    +
    +    def append(self, offset: int, position: int) -> None:
    +        relative = offset - self.base_offset
    +        if relative < 0 or relative > 0xFFFFFFFF:
    +            raise ValueError("relative offset out of range")
    +        if position < 0:
    +            raise ValueError("position cannot be negative")
    +        if self._entries and (
    +            relative <= self._entries[-1][0]
    +            or position <= self._entries[-1][1]
    +        ):
    +            raise ValueError("index entries must be monotonic")
    +        self._file.write(ENTRY.pack(relative, position))
    +        self._entries.append((relative, position))
    +
    +    def floor_position(self, offset: int) -> int:
    +        if not self._entries:
    +            return 0
    +        relative = offset - self.base_offset
    +        index = bisect_right(self._entries, (relative, 2**64 - 1)) - 1
    +        if index < 0:
    +            return 0
    +        return self._entries[index][1]
    +
    +    def truncate_to(self, offset: int) -> None:
    +        keep = 0
    +        for relative, _ in self._entries:
    +            if self.base_offset + relative >= offset:
    +                break
    +            keep += 1
    +        if keep == len(self._entries):
    +            return
    +        del self._entries[keep:]
    +        self._file.truncate(keep * ENTRY.size)
    +        self._file.seek(0, os.SEEK_END)
    +
    +    def flush(self) -> None:
    +        self._file.flush()
    +        os.fsync(self._file.fileno())
    +
    +    def close(self) -> None:
    +        if not self._file.closed:
    +            self._file.close()
    ```

**是什么，为什么现在需要**

稀疏索引把部分相对 Offset 映射到字节位置。Floor Lookup 返回不大于目标的最近位置，日志再从那里向前扫描。

**在运行时做什么**

Append 保证条目单调，Lookup 执行有序 Floor Search，Truncate 让索引与缩短后的日志保持一致。

**关键语句理解**

返回 Floor 而非强求精确命中，让稀疏性仍然正确：索引只缩小扫描范围，不声称定位每条 Record。

#### 包与工程支撑

保持包导出、依赖与测试环境可复现。

??? note "支撑文件差异（1 个文件）"
    **`src/minikafka/log/__init__.py`**

    ```diff
    diff --git a/src/minikafka/log/__init__.py b/src/minikafka/log/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..6092dc28ebd7df90e7293bc98fac2b4ec31a08cd
    --- /dev/null
    +++ b/src/minikafka/log/__init__.py
    @@ -0,0 +1 @@
    +"""Disk-backed partition log components."""
    ```


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/03-offset-index/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

返回 Floor 而非强求精确命中，让稀疏性仍然正确：索引只缩小扫描范围，不声称定位每条 Record。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/02-the-log.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/03-offset-index/stage.patch)
