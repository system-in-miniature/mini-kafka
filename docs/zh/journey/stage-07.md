# Stage 07 · 分区化 Producer Batching

### 目标

实现分区化 Producer Batching，并能从可执行失败、运行时状态与关键语句解释其边界。

??? note "交付文件"
    - `src/minikafka/core/cluster.py`
    - `src/minikafka/errors.py`
    - `src/minikafka/producer/__init__.py`
    - `src/minikafka/producer/accumulator.py`
    - `src/minikafka/producer/partitioner.py`
    - `src/minikafka/producer/producer.py`
    - `tests/producer/test_batching.py`
    - `tests/producer/test_ordering.py`
    - `tests/unit/test_partitioner.py`

### 当前遇到的问题

逐 Record 追加浪费批处理机会；无界缓冲或不稳定分区选择又会破坏延迟与顺序预期。

### 测试契约

#### 先看会坏在哪里

测试分别触发 Linger 与 Batch Size Flush，填满有界缓冲，并混合 Keyed、Keyless 与显式分区发送。

??? note "文件差异：tests/producer/test_batching.py"
    ```diff
    diff --git a/tests/producer/test_batching.py b/tests/producer/test_batching.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..19ff52adbfb2300b6c4396f6f034d0579c68c4c2
    --- /dev/null
    +++ b/tests/producer/test_batching.py
    @@ -0,0 +1,64 @@
    +import asyncio
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.errors import ProducerBufferFull
    +
    +
    +@pytest.mark.asyncio
    +async def test_linger_flushes_one_partition_batch(tmp_path: Path) -> None:
    +    clock = ManualClock(100)
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=clock) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        producer = cluster.producer(batch_size=4096, linger_ms=10)
    +
    +        first = producer.send("events", key=b"k", value=b"1")
    +        second = producer.send("events", key=b"k", value=b"2")
    +        assert not first.done()
    +        clock.advance_ms(10)
    +        await producer.run_due_flushes()
    +
    +        assert (await first).base_offset == 0
    +        assert (await second).offset == 1
    +        assert cluster.debug_batch_count(TopicPartition("events", 0)) == 1
    +
    +
    +@pytest.mark.asyncio
    +async def test_batch_size_flushes_without_waiting_for_linger(tmp_path: Path) -> None:
    +    clock = ManualClock(0)
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=clock) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        producer = cluster.producer(batch_size=68, linger_ms=1000)
    +
    +        first = producer.send("events", value=b"a")
    +        second = producer.send("events", value=b"b")
    +
    +        assert (await first).offset == 0
    +        assert (await second).offset == 1
    +        assert cluster.debug_batch_count(TopicPartition("events", 0)) == 1
    +        await asyncio.wait_for(producer.close(), timeout=0.1)
    +
    +
    +@pytest.mark.asyncio
    +async def test_producer_buffer_is_bounded(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        producer = cluster.producer(
    +            batch_size=4096,
    +            linger_ms=1000,
    +            max_buffer_bytes=40,
    +        )
    +
    +        producer.send("events", value=b"a")
    +        with pytest.raises(ProducerBufferFull):
    +            producer.send("events", value=b"b" * 20)
    +
    +        await producer.flush()
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

测试分别触发 Linger 与 Batch Size Flush，填满有界缓冲，并混合 Keyed、Keyless 与显式分区发送。

**关键测试语句**

```python
assert not first.done()
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

??? note "文件差异：tests/producer/test_ordering.py"
    ```diff
    diff --git a/tests/producer/test_ordering.py b/tests/producer/test_ordering.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..4b3786c29d1538e94af7924d2df902ae897237b1
    --- /dev/null
    +++ b/tests/producer/test_ordering.py
    @@ -0,0 +1,63 @@
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
    +async def test_same_key_keeps_partition_local_order(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 3, 1)
    +        producer = cluster.producer(batch_size=4096, linger_ms=1000)
    +        pending = [
    +            producer.send("events", key=b"user-42", value=str(index).encode())
    +            for index in range(5)
    +        ]
    +
    +        await producer.flush()
    +        metadata = [await item for item in pending]
    +
    +        assert len({item.partition for item in metadata}) == 1
    +        tp = TopicPartition("events", metadata[0].partition)
    +        assert [record.value for record in cluster.leader_log(tp).fetch(0, 10)] == [
    +            b"0",
    +            b"1",
    +            b"2",
    +            b"3",
    +            b"4",
    +        ]
    +
    +
    +@pytest.mark.asyncio
    +async def test_explicit_partition_overrides_keyed_partition(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 3, 1)
    +        producer = cluster.producer(batch_size=1, linger_ms=1000)
    +
    +        metadata = await producer.send(
    +            "events",
    +            key=b"would-hash-elsewhere",
    +            value=b"x",
    +            partition=2,
    +        )
    +
    +        assert metadata.partition == 2
    +
    +
    +@pytest.mark.asyncio
    +async def test_keyless_partition_rotates_after_batch_closes(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 3, 1)
    +        producer = cluster.producer(batch_size=1, linger_ms=1000)
    +
    +        first = await producer.send("events", value=b"a")
    +        second = await producer.send("events", value=b"b")
    +
    +        assert second.partition == (first.partition + 1) % 3
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

测试分别触发 Linger 与 Batch Size Flush，填满有界缓冲，并混合 Keyed、Keyless 与显式分区发送。

**关键测试语句**

```python
assert not first.done()
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

??? note "文件差异：tests/unit/test_partitioner.py"
    ```diff
    diff --git a/tests/unit/test_partitioner.py b/tests/unit/test_partitioner.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..c3b5cc0166b27f7b37f29bec27ade1bbd0057e27
    --- /dev/null
    +++ b/tests/unit/test_partitioner.py
    @@ -0,0 +1,26 @@
    +import zlib
    +
    +from minikafka.producer.partitioner import Partitioner
    +
    +
    +def test_keyed_partition_is_stable() -> None:
    +    partitioner = Partitioner()
    +
    +    assert partitioner.choose(3, key=b"user-42") == (
    +        zlib.crc32(b"user-42") % 3
    +    )
    +    assert partitioner.choose(3, key=b"user-42") == partitioner.choose(
    +        3,
    +        key=b"user-42",
    +    )
    +
    +
    +def test_keyless_partition_is_sticky_until_batch_closes() -> None:
    +    partitioner = Partitioner()
    +
    +    first = partitioner.choose(3, key=None)
    +    assert partitioner.choose(3, key=None) == first
    +
    +    partitioner.on_batch_closed(3, first)
    +
    +    assert partitioner.choose(3, key=None) == (first + 1) % 3
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

测试分别触发 Linger 与 Batch Size Flush，填满有界缓冲，并混合 Keyed、Keyless 与显式分区发送。

**关键测试语句**

```python
assert not first.done()
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Partitioner 选择分区；Accumulator 拥有各分区待发送 Batch；Linger 是延迟边界；Batch Size 是吞吐触发器。

### 为什么需要这个机制

逐 Record 追加浪费批处理机会；无界缓冲或不稳定分区选择又会破坏延迟与顺序预期。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Send 选定一个 Partition、排入 Pending Record，并在大小或时间关闭 Batch 时 Flush；Ack 按分区顺序完成 Record Future。

### 机制板块

#### 分区化 Producer Batching机制

Send 选定一个 Partition、排入 Pending Record，并在大小或时间关闭 Batch 时 Flush；Ack 按分区顺序完成 Record Future。

??? note "文件差异：src/minikafka/core/cluster.py"
    ```diff
    diff --git a/src/minikafka/core/cluster.py b/src/minikafka/core/cluster.py
    index 549d85ae2a71742be015b44d4ebee1fb670ac5ee..16c8bf20542bbcc995f76633ea61f6babae6ccdb 100644
    --- a/src/minikafka/core/cluster.py
    +++ b/src/minikafka/core/cluster.py
    @@ -2,7 +2,9 @@ from __future__ import annotations

     from typing import Self

    +from minikafka.clock import Clock, SystemClock
     from minikafka.config import MiniKafkaConfig
    +from minikafka.core.batch import RecordBatch
     from minikafka.core.metadata import (
         MetadataStore,
         PartitionMetadata,
    @@ -17,6 +19,8 @@ from minikafka.errors import (
         UnknownTopic,
     )
     from minikafka.log.partition_log import PartitionLog
    +from minikafka.log.segment import LocatedBatch
    +from minikafka.producer.producer import Producer


     class BrokerCluster:
    @@ -26,15 +30,23 @@ class BrokerCluster:
             metadata_store: MetadataStore,
             topics: dict[str, TopicMetadata],
             logs: dict[tuple[TopicPartition, int], PartitionLog],
    +        clock: Clock,
         ) -> None:
             self.config = config
             self._metadata_store = metadata_store
             self._topics = topics
             self._logs = logs
    +        self.clock = clock
    +        self._producers: set[Producer] = set()
             self._closed = False

         @classmethod
    -    def open(cls, config: MiniKafkaConfig) -> BrokerCluster:
    +    def open(
    +        cls,
    +        config: MiniKafkaConfig,
    +        *,
    +        clock: Clock | None = None,
    +    ) -> BrokerCluster:
             metadata_store = MetadataStore(config.data_dir / "metadata.json")
             topics = metadata_store.load()
             logs: dict[tuple[TopicPartition, int], PartitionLog] = {}
    @@ -54,7 +66,13 @@ class BrokerCluster:
                 for log in logs.values():
                     log.close()
                 raise
    -        return cls(config, metadata_store, topics, logs)
    +        return cls(
    +            config,
    +            metadata_store,
    +            topics,
    +            logs,
    +            SystemClock() if clock is None else clock,
    +        )

         async def __aenter__(self) -> Self:
             return self
    @@ -143,9 +161,40 @@ class BrokerCluster:
             metadata = self.partition_metadata(tp)
             return self.replica_log(tp, metadata.leader_id)

    +    async def append_leader_batch(
    +        self,
    +        tp: TopicPartition,
    +        batch: RecordBatch,
    +    ) -> LocatedBatch:
    +        self._ensure_open()
    +        return self.leader_log(tp).append(batch)
    +
    +    def producer(
    +        self,
    +        *,
    +        batch_size: int = 16_384,
    +        linger_ms: int = 0,
    +        max_buffer_bytes: int = 1_048_576,
    +    ) -> Producer:
    +        self._ensure_open()
    +        producer = Producer(
    +            self,
    +            self.clock,
    +            batch_size=batch_size,
    +            linger_ms=linger_ms,
    +            max_buffer_bytes=max_buffer_bytes,
    +        )
    +        self._producers.add(producer)
    +        return producer
    +
    +    def debug_batch_count(self, tp: TopicPartition) -> int:
    +        return len(self.leader_log(tp).all_batches())
    +
         async def close(self) -> None:
             if self._closed:
                 return
    +        for producer in tuple(self._producers):
    +            await producer.close()
             for log in self._logs.values():
                 log.flush()
                 log.close()
    ```

??? note "文件差异：src/minikafka/errors.py"
    ```diff
    diff --git a/src/minikafka/errors.py b/src/minikafka/errors.py
    index 0c17241801131b7552aaa3eb229182fe3b570cdd..0fa217d74974398910437c8c805e607ff122560f 100644
    --- a/src/minikafka/errors.py
    +++ b/src/minikafka/errors.py
    @@ -43,3 +43,7 @@ class UnknownTopic(MiniKafkaError):

     class UnknownPartition(MiniKafkaError):
         code = "UNKNOWN_PARTITION"
    +
    +
    +class ProducerBufferFull(MiniKafkaError):
    +    code = "PRODUCER_BUFFER_FULL"
    ```

??? note "文件差异：src/minikafka/producer/accumulator.py"
    ```diff
    diff --git a/src/minikafka/producer/accumulator.py b/src/minikafka/producer/accumulator.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..af617c43c95ef821254f78a3b08bfcdf46d6d670
    --- /dev/null
    +++ b/src/minikafka/producer/accumulator.py
    @@ -0,0 +1,72 @@
    +from __future__ import annotations
    +
    +import asyncio
    +from collections import defaultdict
    +from dataclasses import dataclass
    +
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.core.record import Record
    +from minikafka.errors import ProducerBufferFull
    +
    +
    +@dataclass(slots=True)
    +class PendingRecord:
    +    topic_partition: TopicPartition
    +    record: Record
    +    future: asyncio.Future[object]
    +    estimated_bytes: int
    +
    +
    +class BatchAccumulator:
    +    def __init__(
    +        self,
    +        *,
    +        batch_size: int,
    +        linger_ms: int,
    +        max_buffer_bytes: int,
    +    ) -> None:
    +        if batch_size <= 0:
    +            raise ValueError("batch_size must be positive")
    +        if linger_ms < 0:
    +            raise ValueError("linger_ms cannot be negative")
    +        if max_buffer_bytes <= 0:
    +            raise ValueError("max_buffer_bytes must be positive")
    +        self.batch_size = batch_size
    +        self.linger_ms = linger_ms
    +        self.max_buffer_bytes = max_buffer_bytes
    +        self.total_bytes = 0
    +        self._pending: dict[TopicPartition, list[PendingRecord]] = defaultdict(list)
    +        self._first_enqueue_ms: dict[TopicPartition, int] = {}
    +        self._partition_bytes: dict[TopicPartition, int] = defaultdict(int)
    +
    +    def add(self, pending: PendingRecord, now_ms: int) -> bool:
    +        if self.total_bytes + pending.estimated_bytes > self.max_buffer_bytes:
    +            raise ProducerBufferFull("producer accumulator is full")
    +        tp = pending.topic_partition
    +        if not self._pending[tp]:
    +            self._first_enqueue_ms[tp] = now_ms
    +        self._pending[tp].append(pending)
    +        self._partition_bytes[tp] += pending.estimated_bytes
    +        self.total_bytes += pending.estimated_bytes
    +        return self._partition_bytes[tp] >= self.batch_size
    +
    +    def due_partitions(self, now_ms: int) -> tuple[TopicPartition, ...]:
    +        return tuple(
    +            sorted(
    +                tp
    +                for tp, first_ms in self._first_enqueue_ms.items()
    +                if self._pending[tp] and now_ms - first_ms >= self.linger_ms
    +            )
    +        )
    +
    +    def partitions(self) -> tuple[TopicPartition, ...]:
    +        return tuple(sorted(tp for tp, records in self._pending.items() if records))
    +
    +    def pop(self, tp: TopicPartition) -> tuple[PendingRecord, ...]:
    +        records = tuple(self._pending.pop(tp, ()))
    +        if not records:
    +            return ()
    +        size = self._partition_bytes.pop(tp)
    +        self.total_bytes -= size
    +        self._first_enqueue_ms.pop(tp, None)
    +        return records
    ```

??? note "文件差异：src/minikafka/producer/partitioner.py"
    ```diff
    diff --git a/src/minikafka/producer/partitioner.py b/src/minikafka/producer/partitioner.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..3a1024b016afbf1b78313aeb7edb6b1fd3fcfc03
    --- /dev/null
    +++ b/src/minikafka/producer/partitioner.py
    @@ -0,0 +1,19 @@
    +from __future__ import annotations
    +
    +import zlib
    +
    +
    +class Partitioner:
    +    def __init__(self) -> None:
    +        self._sticky: dict[int, int] = {}
    +
    +    def choose(self, partition_count: int, key: bytes | None) -> int:
    +        if partition_count <= 0:
    +            raise ValueError("partition_count must be positive")
    +        if key is not None:
    +            return zlib.crc32(key) % partition_count
    +        return self._sticky.setdefault(partition_count, 0)
    +
    +    def on_batch_closed(self, partition_count: int, partition: int) -> None:
    +        if self._sticky.get(partition_count) == partition:
    +            self._sticky[partition_count] = (partition + 1) % partition_count
    ```

??? note "文件差异：src/minikafka/producer/producer.py"
    ```diff
    diff --git a/src/minikafka/producer/producer.py b/src/minikafka/producer/producer.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..6009cdff2b9ebc3dbff3b311806c3f6efff9322a
    --- /dev/null
    +++ b/src/minikafka/producer/producer.py
    @@ -0,0 +1,141 @@
    +from __future__ import annotations
    +
    +import asyncio
    +from collections.abc import Iterable
    +from dataclasses import dataclass
    +
    +from minikafka.clock import Clock
    +from minikafka.core.batch import RecordBatch
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.core.record import Header, Record
    +from minikafka.errors import UnknownPartition
    +from minikafka.producer.accumulator import BatchAccumulator, PendingRecord
    +from minikafka.producer.partitioner import Partitioner
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RecordMetadata:
    +    topic: str
    +    partition: int
    +    offset: int
    +    base_offset: int
    +    timestamp_ms: int
    +
    +
    +class Producer:
    +    def __init__(
    +        self,
    +        cluster: object,
    +        clock: Clock,
    +        *,
    +        batch_size: int,
    +        linger_ms: int,
    +        max_buffer_bytes: int,
    +    ) -> None:
    +        self.cluster = cluster
    +        self.clock = clock
    +        self.partitioner = Partitioner()
    +        self.accumulator = BatchAccumulator(
    +            batch_size=batch_size,
    +            linger_ms=linger_ms,
    +            max_buffer_bytes=max_buffer_bytes,
    +        )
    +        self._tasks: set[asyncio.Task[None]] = set()
    +        self._closed = False
    +
    +    def send(
    +        self,
    +        topic: str,
    +        *,
    +        value: bytes | None,
    +        key: bytes | None = None,
    +        timestamp_ms: int | None = None,
    +        headers: Iterable[Header] = (),
    +        partition: int | None = None,
    +    ) -> asyncio.Future[RecordMetadata]:
    +        if self._closed:
    +            raise RuntimeError("producer is closed")
    +        topic_metadata = self.cluster.topic(topic)
    +        partition_count = len(topic_metadata.partitions)
    +        selected = (
    +            self.partitioner.choose(partition_count, key)
    +            if partition is None
    +            else partition
    +        )
    +        if selected not in topic_metadata.partitions:
    +            raise UnknownPartition(f"{topic}-{selected}")
    +        tp = TopicPartition(topic, selected)
    +        timestamp = self.clock.now_ms() if timestamp_ms is None else timestamp_ms
    +        record = Record(key, value, timestamp, tuple(headers))
    +        loop = asyncio.get_running_loop()
    +        future: asyncio.Future[RecordMetadata] = loop.create_future()
    +        estimated_bytes = 39 + len(key or b"") + len(value or b"")
    +        pending = PendingRecord(
    +            topic_partition=tp,
    +            record=record,
    +            future=future,
    +            estimated_bytes=estimated_bytes,
    +        )
    +        full = self.accumulator.add(pending, self.clock.now_ms())
    +        if full:
    +            self._schedule_flush(tp)
    +        return future
    +
    +    def _schedule_flush(self, tp: TopicPartition) -> None:
    +        task = asyncio.create_task(self._flush_partition(tp))
    +        self._tasks.add(task)
    +        task.add_done_callback(self._tasks.discard)
    +
    +    async def _flush_partition(self, tp: TopicPartition) -> None:
    +        pending = self.accumulator.pop(tp)
    +        if not pending:
    +            return
    +        batch = RecordBatch.unassigned(
    +            tuple(item.record for item in pending),
    +        )
    +        try:
    +            appended = await self.cluster.append_leader_batch(tp, batch)
    +        except Exception as error:  # noqa: BLE001 - forward batch failure
    +            for item in pending:
    +                if not item.future.done():
    +                    item.future.set_exception(error)
    +            return
    +        if appended.batch.base_offset is None:
    +            raise RuntimeError("leader appended an unassigned batch")
    +        for delta, item in enumerate(pending):
    +            if not item.future.done():
    +                item.future.set_result(
    +                    RecordMetadata(
    +                        topic=tp.topic,
    +                        partition=tp.partition,
    +                        offset=appended.batch.base_offset + delta,
    +                        base_offset=appended.batch.base_offset,
    +                        timestamp_ms=item.record.timestamp_ms,
    +                    )
    +                )
    +        if any(item.record.key is None for item in pending):
    +            self.partitioner.on_batch_closed(
    +                len(self.cluster.topic(tp.topic).partitions),
    +                tp.partition,
    +            )
    +
    +    async def run_due_flushes(self) -> None:
    +        due = self.accumulator.due_partitions(self.clock.now_ms())
    +        await asyncio.gather(*(self._flush_partition(tp) for tp in due))
    +
    +    async def flush(self) -> None:
    +        while self.accumulator.partitions() or self._tasks:
    +            self._tasks = {task for task in self._tasks if not task.done()}
    +            partitions = self.accumulator.partitions()
    +            if partitions:
    +                await asyncio.gather(
    +                    *(self._flush_partition(tp) for tp in partitions)
    +                )
    +            if self._tasks:
    +                await asyncio.gather(*tuple(self._tasks))
    +
    +    async def close(self) -> None:
    +        if self._closed:
    +            return
    +        await self.flush()
    +        self._closed = True
    ```

**是什么，为什么现在需要**

Partitioner 选择分区；Accumulator 拥有各分区待发送 Batch；Linger 是延迟边界；Batch Size 是吞吐触发器。

**在运行时做什么**

Send 选定一个 Partition、排入 Pending Record，并在大小或时间关闭 Batch 时 Flush；Ack 按分区顺序完成 Record Future。

**关键语句理解**

Keyed Record 稳定哈希，打开的 Keyless Batch 保持 Sticky，因此批处理效率不会悄悄改变同分区顺序。

#### 包与工程支撑

保持包导出、依赖与测试环境可复现。

??? note "支撑文件差异（1 个文件）"
    **`src/minikafka/producer/__init__.py`**

    ```diff
    diff --git a/src/minikafka/producer/__init__.py b/src/minikafka/producer/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..e0d7b412683943886d1e9f00209f3ead0235e8f8
    --- /dev/null
    +++ b/src/minikafka/producer/__init__.py
    @@ -0,0 +1 @@
    +"""Producer partitioning, batching, and retry state."""
    ```


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/07-producer-batching/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

Keyed Record 稳定哈希，打开的 Keyless Batch 保持 Sticky，因此批处理效率不会悄悄改变同分区顺序。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 4 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/04-producer.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/07-producer-batching/stage.patch)
