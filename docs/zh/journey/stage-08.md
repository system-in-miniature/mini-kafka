# Stage 08 · Consumer Position 与重放

### 目标

实现Consumer Position 与重放，并能从可执行失败、运行时状态与关键语句解释其边界。

??? note "交付文件"
    - `src/minikafka/consumer/__init__.py`
    - `src/minikafka/consumer/consumer.py`
    - `src/minikafka/consumer/offsets.py`
    - `src/minikafka/core/cluster.py`
    - `tests/consumer/test_delivery_semantics.py`
    - `tests/consumer/test_positions.py`
    - `tests/reliability/test_offset_restart.py`

### 当前遇到的问题

读取 Record 与声明处理完成是两个事件；把它们合并会让崩溃行为无法控制。

### 测试契约

#### 先看会坏在哪里

投递测试分别在 Commit 前后模拟崩溃，让 Replay 与 Skip 行为直接可见。

??? note "文件差异：tests/consumer/test_delivery_semantics.py"
    ```diff
    diff --git a/tests/consumer/test_delivery_semantics.py b/tests/consumer/test_delivery_semantics.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..60d49f65062dc73ab7bd37a66d59376ee9f302b1
    --- /dev/null
    +++ b/tests/consumer/test_delivery_semantics.py
    @@ -0,0 +1,46 @@
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.core.metadata import TopicPartition
    +
    +
    +@pytest.mark.asyncio
    +async def test_process_before_commit_can_replay_after_crash(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    tp = TopicPartition("events", 0)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        await cluster.producer(batch_size=1).send("events", value=b"work")
    +        first = cluster.consumer(group_id="workers")
    +        first.assign((tp,))
    +        assert (await first.poll(1))[0].value == b"work"
    +        # Processing happened, but the next offset was not committed.
    +
    +        restarted = cluster.consumer(group_id="workers")
    +        restarted.assign((tp,))
    +        replayed = await restarted.poll(1)
    +
    +        assert replayed[0].offset == 0
    +
    +
    +@pytest.mark.asyncio
    +async def test_commit_before_process_can_skip_after_crash(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    tp = TopicPartition("events", 0)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        await cluster.producer(batch_size=1).send("events", value=b"work")
    +        first = cluster.consumer(group_id="workers")
    +        first.assign((tp,))
    +        await first.poll(1)
    +        await first.commit()
    +        # Crash occurs before application processing.
    +
    +        restarted = cluster.consumer(group_id="workers")
    +        restarted.assign((tp,))
    +
    +        assert await restarted.poll(1) == ()
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

投递测试分别在 Commit 前后模拟崩溃，让 Replay 与 Skip 行为直接可见。

**关键测试语句**

```python
assert (await first.poll(1))[0].value == b"work"
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

??? note "文件差异：tests/consumer/test_positions.py"
    ```diff
    diff --git a/tests/consumer/test_positions.py b/tests/consumer/test_positions.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..7cc174d2dd66462206c9ac3e3e4f8e422c5ba7b2
    --- /dev/null
    +++ b/tests/consumer/test_positions.py
    @@ -0,0 +1,71 @@
    +import asyncio
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.core.metadata import TopicPartition
    +
    +
    +async def produce_values(cluster: BrokerCluster, values: list[bytes]) -> None:
    +    producer = cluster.producer(batch_size=4096, linger_ms=1000)
    +    pending = [producer.send("events", value=value) for value in values]
    +    await producer.flush()
    +    await asyncio.gather(*pending)
    +
    +
    +@pytest.mark.asyncio
    +async def test_position_and_commit_are_independent(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        await produce_values(cluster, [b"0", b"1", b"2"])
    +        tp = TopicPartition("events", 0)
    +        consumer = cluster.consumer(group_id="g", auto_offset_reset="earliest")
    +        consumer.assign((tp,))
    +
    +        records = await consumer.poll(max_records=2)
    +
    +        assert [record.offset for record in records] == [0, 1]
    +        assert consumer.position(tp) == 2
    +        assert await consumer.committed(tp) is None
    +        assert await consumer.lag(tp) == 1
    +
    +        await consumer.commit()
    +        assert await consumer.committed(tp) == 2
    +        consumer.seek(tp, 0)
    +        assert (await consumer.poll(1))[0].offset == 0
    +
    +
    +@pytest.mark.asyncio
    +async def test_latest_consumer_starts_at_log_end(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        await produce_values(cluster, [b"old"])
    +        tp = TopicPartition("events", 0)
    +        consumer = cluster.consumer(group_id="latest", auto_offset_reset="latest")
    +        consumer.assign((tp,))
    +
    +        assert await consumer.poll(1) == ()
    +        assert consumer.position(tp) == 1
    +
    +
    +@pytest.mark.asyncio
    +async def test_poll_distributes_record_budget_across_assignment(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 2, 1)
    +        producer = cluster.producer(batch_size=1)
    +        await producer.send("events", value=b"p0", partition=0)
    +        await producer.send("events", value=b"p1", partition=1)
    +        consumer = cluster.consumer(group_id="g")
    +        consumer.assign(
    +            (TopicPartition("events", 0), TopicPartition("events", 1))
    +        )
    +
    +        records = await consumer.poll(max_records=1)
    +
    +        assert len(records) == 1
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

投递测试分别在 Commit 前后模拟崩溃，让 Replay 与 Skip 行为直接可见。

**关键测试语句**

```python
assert (await first.poll(1))[0].value == b"work"
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

??? note "文件差异：tests/reliability/test_offset_restart.py"
    ```diff
    diff --git a/tests/reliability/test_offset_restart.py b/tests/reliability/test_offset_restart.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..38582078360106a3f8e1d5003af607bcd4e4f1fe
    --- /dev/null
    +++ b/tests/reliability/test_offset_restart.py
    @@ -0,0 +1,28 @@
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.core.metadata import TopicPartition
    +
    +
    +@pytest.mark.asyncio
    +async def test_committed_offset_survives_cluster_restart(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    tp = TopicPartition("events", 0)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        await cluster.producer(batch_size=1).send("events", value=b"one")
    +        consumer = cluster.consumer(group_id="g")
    +        consumer.assign((tp,))
    +        await consumer.poll(1)
    +        await consumer.commit()
    +
    +    async with BrokerCluster.open(config, clock=ManualClock()) as reopened:
    +        consumer = reopened.consumer(group_id="g")
    +        consumer.assign((tp,))
    +
    +        assert await consumer.committed(tp) == 1
    +        assert await consumer.poll(1) == ()
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

投递测试分别在 Commit 前后模拟崩溃，让 Replay 与 Skip 行为直接可见。

**关键测试语句**

```python
assert (await first.poll(1))[0].value == b"work"
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Position 是单个 Consumer 实例的下一 Offset；Committed Offset 是持久 Group 进度；只有不存在 Commit 时，Earliest/Latest 才提供起点。

### 为什么需要这个机制

读取 Record 与声明处理完成是两个事件；把它们合并会让崩溃行为无法控制。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Poll 返回记录时推进本地 Position；Commit 持久化选定 Next Offset；重开 Consumer 从 Commit 或 Reset Policy 初始化。

### 机制板块

#### Consumer Position 与重放机制

Poll 返回记录时推进本地 Position；Commit 持久化选定 Next Offset；重开 Consumer 从 Commit 或 Reset Policy 初始化。

??? note "文件差异：src/minikafka/consumer/consumer.py"
    ```diff
    diff --git a/src/minikafka/consumer/consumer.py b/src/minikafka/consumer/consumer.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..a73ef6cd391ba899af80373901d2cceb0f03c1af
    --- /dev/null
    +++ b/src/minikafka/consumer/consumer.py
    @@ -0,0 +1,126 @@
    +from __future__ import annotations
    +
    +from collections.abc import Iterable
    +from typing import TYPE_CHECKING
    +
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.core.record import StoredRecord
    +from minikafka.errors import OffsetOutOfRange
    +
    +if TYPE_CHECKING:
    +    from minikafka.core.cluster import BrokerCluster
    +
    +
    +class Consumer:
    +    def __init__(
    +        self,
    +        cluster: BrokerCluster,
    +        group_id: str,
    +        *,
    +        auto_offset_reset: str,
    +    ) -> None:
    +        if not group_id:
    +            raise ValueError("group_id cannot be empty")
    +        if auto_offset_reset not in {"earliest", "latest", "error"}:
    +            raise ValueError("auto_offset_reset must be earliest, latest, or error")
    +        self.cluster = cluster
    +        self.group_id = group_id
    +        self.auto_offset_reset = auto_offset_reset
    +        self._assignment: tuple[TopicPartition, ...] = ()
    +        self._positions: dict[TopicPartition, int] = {}
    +        self._closed = False
    +
    +    @property
    +    def assignment(self) -> tuple[TopicPartition, ...]:
    +        return self._assignment
    +
    +    def assign(self, partitions: Iterable[TopicPartition]) -> None:
    +        assigned = tuple(sorted(set(partitions)))
    +        for tp in assigned:
    +            self.cluster.partition_metadata(tp)
    +        self._assignment = assigned
    +        self._positions = {
    +            tp: position
    +            for tp, position in self._positions.items()
    +            if tp in assigned
    +        }
    +
    +    async def _initial_position(self, tp: TopicPartition) -> int:
    +        committed = await self.committed(tp)
    +        if committed is not None:
    +            return committed
    +        if self.auto_offset_reset == "earliest":
    +            return self.cluster.log_start_offset(tp)
    +        if self.auto_offset_reset == "latest":
    +            return self.cluster.visible_end(tp)
    +        raise OffsetOutOfRange(
    +            -1,
    +            self.cluster.log_start_offset(tp),
    +            self.cluster.visible_end(tp),
    +        )
    +
    +    async def poll(self, max_records: int) -> tuple[StoredRecord, ...]:
    +        if self._closed:
    +            raise RuntimeError("consumer is closed")
    +        if max_records < 0:
    +            raise ValueError("max_records cannot be negative")
    +        result: list[StoredRecord] = []
    +        for tp in self._assignment:
    +            if len(result) >= max_records:
    +                break
    +            position = self._positions.get(tp)
    +            if position is None:
    +                position = await self._initial_position(tp)
    +                self._positions[tp] = position
    +            try:
    +                fetched = self.cluster.fetch(
    +                    tp,
    +                    position,
    +                    max_records - len(result),
    +                )
    +            except OffsetOutOfRange:
    +                if self.auto_offset_reset == "error":
    +                    raise
    +                position = (
    +                    self.cluster.log_start_offset(tp)
    +                    if self.auto_offset_reset == "earliest"
    +                    else self.cluster.visible_end(tp)
    +                )
    +                self._positions[tp] = position
    +                fetched = self.cluster.fetch(
    +                    tp,
    +                    position,
    +                    max_records - len(result),
    +                )
    +            if fetched:
    +                self._positions[tp] = fetched[-1].offset + 1
    +                result.extend(fetched)
    +        return tuple(result)
    +
    +    def position(self, tp: TopicPartition) -> int:
    +        try:
    +            return self._positions[tp]
    +        except KeyError as error:
    +            raise ValueError(f"no position initialized for {tp}") from error
    +
    +    def seek(self, tp: TopicPartition, offset: int) -> None:
    +        if tp not in self._assignment:
    +            raise ValueError(f"{tp} is not assigned")
    +        if offset < 0:
    +            raise ValueError("offset cannot be negative")
    +        self._positions[tp] = offset
    +
    +    async def committed(self, tp: TopicPartition) -> int | None:
    +        return await self.cluster.offsets.get(self.group_id, tp)
    +
    +    async def commit(self) -> None:
    +        await self.cluster.offsets.commit(
    +            self.group_id,
    +            dict(self._positions),
    +        )
    +
    +    async def lag(self, tp: TopicPartition) -> int:
    +        return max(0, self.cluster.visible_end(tp) - self.position(tp))
    +
    +    async def close(self) -> None:
    +        self._closed = True
    ```

??? note "文件差异：src/minikafka/consumer/offsets.py"
    ```diff
    diff --git a/src/minikafka/consumer/offsets.py b/src/minikafka/consumer/offsets.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..4fc2a921d5ea5949b86efef3cd6310b9e7deee3e
    --- /dev/null
    +++ b/src/minikafka/consumer/offsets.py
    @@ -0,0 +1,86 @@
    +from __future__ import annotations
    +
    +import json
    +import os
    +from pathlib import Path
    +
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.errors import StorageError
    +
    +
    +class OffsetStore:
    +    def __init__(
    +        self,
    +        path: Path,
    +        offsets: dict[str, dict[TopicPartition, int]],
    +    ) -> None:
    +        self.path = path
    +        self._offsets = offsets
    +
    +    @classmethod
    +    def open(cls, path: Path) -> OffsetStore:
    +        if not path.exists():
    +            return cls(path, {})
    +        try:
    +            raw = json.loads(path.read_text())
    +            offsets = {
    +                group_id: {
    +                    TopicPartition(item["topic"], item["partition"]): item["offset"]
    +                    for item in entries
    +                }
    +                for group_id, entries in raw["groups"].items()
    +            }
    +        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
    +            raise StorageError(f"invalid offset file {path}") from error
    +        return cls(path, offsets)
    +
    +    async def get(self, group_id: str, tp: TopicPartition) -> int | None:
    +        return self._offsets.get(group_id, {}).get(tp)
    +
    +    async def commit(
    +        self,
    +        group_id: str,
    +        offsets: dict[TopicPartition, int],
    +    ) -> None:
    +        if any(offset < 0 for offset in offsets.values()):
    +            raise ValueError("committed offsets cannot be negative")
    +        self._offsets.setdefault(group_id, {}).update(offsets)
    +        self.flush()
    +
    +    async def commit_many(
    +        self,
    +        groups: dict[str, dict[TopicPartition, int]],
    +    ) -> None:
    +        for group_id, offsets in groups.items():
    +            if any(offset < 0 for offset in offsets.values()):
    +                raise ValueError("committed offsets cannot be negative")
    +            self._offsets.setdefault(group_id, {}).update(offsets)
    +        self.flush()
    +
    +    def flush(self) -> None:
    +        self.path.parent.mkdir(parents=True, exist_ok=True)
    +        payload = {
    +            "version": 1,
    +            "groups": {
    +                group_id: [
    +                    {
    +                        "topic": tp.topic,
    +                        "partition": tp.partition,
    +                        "offset": offset,
    +                    }
    +                    for tp, offset in sorted(offsets.items())
    +                ]
    +                for group_id, offsets in sorted(self._offsets.items())
    +            },
    +        }
    +        temporary = self.path.with_suffix(".tmp")
    +        with temporary.open("w") as file:
    +            json.dump(payload, file, sort_keys=True, separators=(",", ":"))
    +            file.flush()
    +            os.fsync(file.fileno())
    +        os.replace(temporary, self.path)
    +        directory_fd = os.open(self.path.parent, os.O_RDONLY)
    +        try:
    +            os.fsync(directory_fd)
    +        finally:
    +            os.close(directory_fd)
    ```

??? note "文件差异：src/minikafka/core/cluster.py"
    ```diff
    diff --git a/src/minikafka/core/cluster.py b/src/minikafka/core/cluster.py
    index 16c8bf20542bbcc995f76633ea61f6babae6ccdb..5154543aa15d013bf2b1523d0181098e28cc1a9c 100644
    --- a/src/minikafka/core/cluster.py
    +++ b/src/minikafka/core/cluster.py
    @@ -4,6 +4,8 @@ from typing import Self

     from minikafka.clock import Clock, SystemClock
     from minikafka.config import MiniKafkaConfig
    +from minikafka.consumer.consumer import Consumer
    +from minikafka.consumer.offsets import OffsetStore
     from minikafka.core.batch import RecordBatch
     from minikafka.core.metadata import (
         MetadataStore,
    @@ -13,6 +15,7 @@ from minikafka.core.metadata import (
         round_robin_replicas,
         validate_topic_name,
     )
    +from minikafka.core.record import StoredRecord
     from minikafka.errors import (
         TopicAlreadyExists,
         UnknownPartition,
    @@ -31,13 +34,16 @@ class BrokerCluster:
             topics: dict[str, TopicMetadata],
             logs: dict[tuple[TopicPartition, int], PartitionLog],
             clock: Clock,
    +        offsets: OffsetStore,
         ) -> None:
             self.config = config
             self._metadata_store = metadata_store
             self._topics = topics
             self._logs = logs
             self.clock = clock
    +        self.offsets = offsets
             self._producers: set[Producer] = set()
    +        self._consumers: set[Consumer] = set()
             self._closed = False

         @classmethod
    @@ -72,6 +78,7 @@ class BrokerCluster:
                 topics,
                 logs,
                 SystemClock() if clock is None else clock,
    +            OffsetStore.open(config.data_dir / "offsets.json"),
             )

         async def __aenter__(self) -> Self:
    @@ -190,11 +197,58 @@ class BrokerCluster:
         def debug_batch_count(self, tp: TopicPartition) -> int:
             return len(self.leader_log(tp).all_batches())

    +    def fetch(
    +        self,
    +        tp: TopicPartition,
    +        offset: int,
    +        max_records: int,
    +    ) -> tuple[StoredRecord, ...]:
    +        return tuple(
    +            StoredRecord(
    +                topic=tp.topic,
    +                partition=tp.partition,
    +                offset=record.offset,
    +                key=record.key,
    +                value=record.value,
    +                timestamp_ms=record.timestamp_ms,
    +                headers=record.headers,
    +            )
    +            for record in self.leader_log(tp).fetch(
    +                offset,
    +                max_records,
    +                end_offset=self.visible_end(tp),
    +            )
    +        )
    +
    +    def visible_end(self, tp: TopicPartition) -> int:
    +        return self.leader_log(tp).leo
    +
    +    def log_start_offset(self, tp: TopicPartition) -> int:
    +        return self.leader_log(tp).log_start_offset
    +
    +    def consumer(
    +        self,
    +        *,
    +        group_id: str,
    +        auto_offset_reset: str = "earliest",
    +    ) -> Consumer:
    +        self._ensure_open()
    +        consumer = Consumer(
    +            self,
    +            group_id,
    +            auto_offset_reset=auto_offset_reset,
    +        )
    +        self._consumers.add(consumer)
    +        return consumer
    +
         async def close(self) -> None:
             if self._closed:
                 return
             for producer in tuple(self._producers):
                 await producer.close()
    +        for consumer in tuple(self._consumers):
    +            await consumer.close()
    +        self.offsets.flush()
             for log in self._logs.values():
                 log.flush()
                 log.close()
    ```

**是什么，为什么现在需要**

Position 是单个 Consumer 实例的下一 Offset；Committed Offset 是持久 Group 进度；只有不存在 Commit 时，Earliest/Latest 才提供起点。

**在运行时做什么**

Poll 返回记录时推进本地 Position；Commit 持久化选定 Next Offset；重开 Consumer 从 Commit 或 Reset Policy 初始化。

**关键语句理解**

Poll 时推进 Position 是安全的，因为持久进度只在 Commit 时变化；这一区分定义了 At-least-once 与 At-most-once 的失败窗口。

#### 包与工程支撑

保持包导出、依赖与测试环境可复现。

??? note "支撑文件差异（1 个文件）"
    **`src/minikafka/consumer/__init__.py`**

    ```diff
    diff --git a/src/minikafka/consumer/__init__.py b/src/minikafka/consumer/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..1a09474fab1b328a575723668ef89d17bc594efa
    --- /dev/null
    +++ b/src/minikafka/consumer/__init__.py
    @@ -0,0 +1 @@
    +"""Consumer positions, offsets, and group coordination."""
    ```


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/08-consumer-positions/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

Poll 时推进 Position 是安全的，因为持久进度只在 Commit 时变化；这一区分定义了 At-least-once 与 At-most-once 的失败窗口。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 7 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/07-consumer-groups.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/08-consumer-positions/stage.patch)
