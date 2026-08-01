# Stage 16 · 事务化 Record 与 Offset

### 目标

实现事务化 Record 与 Offset，并能从可执行失败、运行时状态与关键语句解释其边界。

??? note "交付文件"
    - `src/minikafka/consumer/offsets.py`
    - `src/minikafka/core/cluster.py`
    - `src/minikafka/transaction/__init__.py`
    - `src/minikafka/transaction/journal.py`
    - `src/minikafka/transaction/manager.py`
    - `src/minikafka/transaction/model.py`
    - `tests/reliability/test_transaction_restart.py`
    - `tests/transaction/test_abort.py`
    - `tests/transaction/test_offsets.py`
    - `tests/transaction/test_visibility.py`

### 当前遇到的问题

分别发布输出与提交输入进度，会在崩溃后暴露部分完成的工作。

### 测试契约

#### 先看会坏在哪里

可见性测试停在 Prepare 与 Commit 之间；Abort 测试隐藏数据；Offset 测试让输出与输入进度一起发布；Journal 测试截断持久尾部。

??? note "文件差异：tests/reliability/test_transaction_restart.py"
    ```diff
    diff --git a/tests/reliability/test_transaction_restart.py b/tests/reliability/test_transaction_restart.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..9a842bd7aa9c7a2420f347c4d4eb0859b830413a
    --- /dev/null
    +++ b/tests/reliability/test_transaction_restart.py
    @@ -0,0 +1,62 @@
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.replication.model import IsolationLevel
    +from minikafka.transaction.journal import TransactionJournal
    +from minikafka.transaction.model import TransactionData, TransactionState
    +
    +
    +@pytest.mark.asyncio
    +async def test_committed_transaction_visibility_survives_restart(
    +    tmp_path: Path,
    +) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    tp = TopicPartition("out", 0)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("out", 1, 1)
    +        tx = await cluster.transactions.begin("durable")
    +        await tx.send("out", value=b"kept")
    +        await tx.commit()
    +
    +    async with BrokerCluster.open(config, clock=ManualClock()) as reopened:
    +        assert [r.value for r in reopened.fetch(
    +            tp, 0, 10, IsolationLevel.READ_COMMITTED
    +        )] == [b"kept"]
    +
    +
    +@pytest.mark.asyncio
    +async def test_prepare_commit_finishes_offsets_during_recovery(
    +    tmp_path: Path,
    +) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    input_tp = TopicPartition("input", 0)
    +    journal = TransactionJournal(tmp_path / "transactions.journal")
    +    journal.append(
    +        TransactionData(
    +            "recover-me",
    +            state=TransactionState.PREPARE_COMMIT,
    +            staged_offsets={"workers": {input_tp: 7}},
    +        )
    +    )
    +
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        assert await cluster.offsets.get("workers", input_tp) == 7
    +
    +
    +def test_transaction_journal_truncates_incomplete_tail(tmp_path: Path) -> None:
    +    path = tmp_path / "transactions.journal"
    +    journal = TransactionJournal(path)
    +    journal.append(TransactionData("valid"))
    +    valid_size = path.stat().st_size
    +    with path.open("ab") as file:
    +        file.write(b"deadbeef {")
    +
    +    recovered = journal.recover()
    +
    +    assert set(recovered) == {"valid"}
    +    assert path.stat().st_size == valid_size
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

可见性测试停在 Prepare 与 Commit 之间；Abort 测试隐藏数据；Offset 测试让输出与输入进度一起发布；Journal 测试截断持久尾部。

**关键测试语句**

```python
assert [r.value for r in reopened.fetch(
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

??? note "文件差异：tests/transaction/test_abort.py"
    ```diff
    diff --git a/tests/transaction/test_abort.py b/tests/transaction/test_abort.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..d8da0afd30ac675464d6d9d0b1a763b36e6455fb
    --- /dev/null
    +++ b/tests/transaction/test_abort.py
    @@ -0,0 +1,26 @@
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.replication.model import IsolationLevel
    +
    +
    +@pytest.mark.asyncio
    +async def test_aborted_records_remain_hidden(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("out", 1, 1)
    +        tx = await cluster.transactions.begin("tx-2")
    +        await tx.send("out", value=b"discard")
    +        await tx.abort()
    +
    +        assert cluster.fetch(
    +            TopicPartition("out", 0),
    +            0,
    +            10,
    +            IsolationLevel.READ_COMMITTED,
    +        ) == ()
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

可见性测试停在 Prepare 与 Commit 之间；Abort 测试隐藏数据；Offset 测试让输出与输入进度一起发布；Journal 测试截断持久尾部。

**关键测试语句**

```python
assert [r.value for r in reopened.fetch(
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

??? note "文件差异：tests/transaction/test_offsets.py"
    ```diff
    diff --git a/tests/transaction/test_offsets.py b/tests/transaction/test_offsets.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..bdb2cf18be4eb77aed16754fa31e62922a35486f
    --- /dev/null
    +++ b/tests/transaction/test_offsets.py
    @@ -0,0 +1,39 @@
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
    +async def test_output_and_input_offset_publish_together(
    +    tmp_path: Path,
    +) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    input_tp = TopicPartition("input", 0)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("input", 1, 1)
    +        await cluster.create_topic("output", 1, 1)
    +        tx = await cluster.transactions.begin("processor")
    +        await tx.send("output", value=b"result")
    +        await tx.send_offsets("workers", {input_tp: 4})
    +        assert await cluster.offsets.get("workers", input_tp) is None
    +
    +        await tx.commit()
    +
    +        assert await cluster.offsets.get("workers", input_tp) == 4
    +
    +
    +@pytest.mark.asyncio
    +async def test_abort_discards_staged_offsets(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    input_tp = TopicPartition("input", 0)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("input", 1, 1)
    +        tx = await cluster.transactions.begin("processor")
    +        await tx.send_offsets("workers", {input_tp: 4})
    +        await tx.abort()
    +        assert await cluster.offsets.get("workers", input_tp) is None
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

可见性测试停在 Prepare 与 Commit 之间；Abort 测试隐藏数据；Offset 测试让输出与输入进度一起发布；Journal 测试截断持久尾部。

**关键测试语句**

```python
assert [r.value for r in reopened.fetch(
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

??? note "文件差异：tests/transaction/test_visibility.py"
    ```diff
    diff --git a/tests/transaction/test_visibility.py b/tests/transaction/test_visibility.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..a82cd2c7737d10196e27d99f4669b7a023533a1b
    --- /dev/null
    +++ b/tests/transaction/test_visibility.py
    @@ -0,0 +1,32 @@
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.replication.model import IsolationLevel
    +
    +
    +@pytest.mark.asyncio
    +async def test_read_committed_waits_for_commit_marker(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("out", 1, 1)
    +        tx = await cluster.transactions.begin("tx-1")
    +        await tx.send("out", value=b"pending")
    +        tp = TopicPartition("out", 0)
    +
    +        assert [r.value for r in cluster.fetch(
    +            tp, 0, 10, IsolationLevel.READ_UNCOMMITTED
    +        )] == [b"pending"]
    +        assert cluster.fetch(tp, 0, 10, IsolationLevel.READ_COMMITTED) == ()
    +
    +        await tx.commit()
    +
    +        assert [r.value for r in cluster.fetch(
    +            tp, 0, 10, IsolationLevel.READ_COMMITTED
    +        )] == [b"pending"]
    +        assert all(batch.control is not None for batch in
    +                   cluster.leader_log(tp).all_batches()[1:])
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

可见性测试停在 Prepare 与 Commit 之间；Abort 测试隐藏数据；Offset 测试让输出与输入进度一起发布；Journal 测试截断持久尾部。

**关键测试语句**

```python
assert [r.value for r in reopened.fetch(
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Transaction 具有 Epoch 与 State；Control Batch 标记 Commit 或 Abort；`read_committed` 过滤未决与已中止数据；Journal 驱动 Prepared Decision 恢复。

### 为什么需要这个机制

分别发布输出与提交输入进度，会在崩溃后暴露部分完成的工作。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Begin Fencing 旧 Epoch；Send 追加事务 Batch；Prepare 持久记录意图；Commit 写 Marker 与 Offset；Recovery 确定性完成或中止未完成工作。

### 机制板块

#### 事务化 Record 与 Offset机制

Begin Fencing 旧 Epoch；Send 追加事务 Batch；Prepare 持久记录意图；Commit 写 Marker 与 Offset；Recovery 确定性完成或中止未完成工作。

??? note "文件差异：src/minikafka/consumer/offsets.py"
    ```diff
    diff --git a/src/minikafka/consumer/offsets.py b/src/minikafka/consumer/offsets.py
    index 4fc2a921d5ea5949b86efef3cd6310b9e7deee3e..bc4a778975d1512f3d4d46034aeeae4653dca8f3 100644
    --- a/src/minikafka/consumer/offsets.py
    +++ b/src/minikafka/consumer/offsets.py
    @@ -50,6 +50,12 @@ class OffsetStore:
         async def commit_many(
             self,
             groups: dict[str, dict[TopicPartition, int]],
    +    ) -> None:
    +        self.commit_many_sync(groups)
    +
    +    def commit_many_sync(
    +        self,
    +        groups: dict[str, dict[TopicPartition, int]],
         ) -> None:
             for group_id, offsets in groups.items():
                 if any(offset < 0 for offset in offsets.values()):
    ```

??? note "文件差异：src/minikafka/core/cluster.py"
    ```diff
    diff --git a/src/minikafka/core/cluster.py b/src/minikafka/core/cluster.py
    index 8f84bfe5608505d29093390f80badeb9214d5b5f..7d3703a230291df393d1f5270a07704bcbdc87fa 100644
    --- a/src/minikafka/core/cluster.py
    +++ b/src/minikafka/core/cluster.py
    @@ -27,9 +27,11 @@ from minikafka.log.partition_log import PartitionLog
     from minikafka.log.segment import LocatedBatch
     from minikafka.producer.producer import Producer
     from minikafka.producer.state import ProducerIdentityStore
    -from minikafka.replication.model import AckMode
    +from minikafka.replication.model import AckMode, IsolationLevel
     from minikafka.replication.replica import Replica
     from minikafka.replication.replica_set import PartitionReplicaSet
    +from minikafka.transaction.journal import TransactionJournal
    +from minikafka.transaction.manager import TransactionManager


     class BrokerCluster:
    @@ -62,6 +64,10 @@ class BrokerCluster:
             )
             self._replica_sets: dict[TopicPartition, PartitionReplicaSet] = {}
             self._build_replica_sets()
    +        self.transactions = TransactionManager(
    +            self,
    +            TransactionJournal(config.data_dir / "transactions.journal"),
    +        )
             self._closed = False

         @classmethod
    @@ -253,24 +259,47 @@ class BrokerCluster:
             tp: TopicPartition,
             offset: int,
             max_records: int,
    +        isolation: IsolationLevel = IsolationLevel.READ_UNCOMMITTED,
         ) -> tuple[StoredRecord, ...]:
    -        records = self.leader_log(tp).fetch(
    -            offset,
    -            max_records,
    -            end_offset=self.visible_end(tp),
    -        )
    -        return tuple(
    -            StoredRecord(
    -                topic=tp.topic,
    -                partition=tp.partition,
    -                offset=record.offset,
    -                key=record.key,
    -                value=record.value,
    -                timestamp_ms=record.timestamp_ms,
    -                headers=record.headers,
    -            )
    -            for record in records
    -        )
    +        end_offset = self.visible_end(tp)
    +        if isolation is IsolationLevel.READ_COMMITTED:
    +            end_offset = self.transactions.last_stable_offset(tp, end_offset)
    +        stored: list[StoredRecord] = []
    +        for batch in self.leader_log(tp).read_batches(offset, 2**63 - 1):
    +            if batch.base_offset is None or batch.base_offset >= end_offset:
    +                break
    +            if batch.control is not None:
    +                continue
    +            if (
    +                isolation is IsolationLevel.READ_COMMITTED
    +                and batch.transactional_id is not None
    +                and not self.transactions.is_committed(batch.transactional_id)
    +            ):
    +                continue
    +            if batch.offset_deltas is None:
    +                continue
    +            for delta, record in zip(
    +                batch.offset_deltas,
    +                batch.records,
    +                strict=True,
    +            ):
    +                record_offset = batch.base_offset + delta
    +                if record_offset < offset or record_offset >= end_offset:
    +                    continue
    +                stored.append(
    +                    StoredRecord(
    +                        topic=tp.topic,
    +                        partition=tp.partition,
    +                        offset=record_offset,
    +                        key=record.key,
    +                        value=record.value,
    +                        timestamp_ms=record.timestamp_ms,
    +                        headers=record.headers,
    +                    )
    +                )
    +                if len(stored) == max_records:
    +                    return tuple(stored)
    +        return tuple(stored)

         def visible_end(self, tp: TopicPartition) -> int:
             return self.replica_set(tp).high_watermark
    ```

??? note "文件差异：src/minikafka/transaction/journal.py"
    ```diff
    diff --git a/src/minikafka/transaction/journal.py b/src/minikafka/transaction/journal.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..2c3d155d9cb3c960ad736b211803e03e7b1f9728
    --- /dev/null
    +++ b/src/minikafka/transaction/journal.py
    @@ -0,0 +1,87 @@
    +from __future__ import annotations
    +
    +import json
    +import os
    +import zlib
    +from pathlib import Path
    +
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.transaction.model import TransactionData, TransactionState
    +
    +
    +class TransactionJournal:
    +    def __init__(self, path: Path) -> None:
    +        self.path = path
    +
    +    def append(self, transaction: TransactionData) -> None:
    +        self.path.parent.mkdir(parents=True, exist_ok=True)
    +        payload = json.dumps(
    +            {
    +                "id": transaction.transaction_id,
    +                "state": transaction.state.value,
    +                "partitions": [
    +                    [tp.topic, tp.partition]
    +                    for tp in sorted(transaction.partitions)
    +                ],
    +                "first_offsets": [
    +                    [tp.topic, tp.partition, offset]
    +                    for tp, offset in sorted(transaction.first_offsets.items())
    +                ],
    +                "staged_offsets": {
    +                    group: [
    +                        [tp.topic, tp.partition, offset]
    +                        for tp, offset in sorted(offsets.items())
    +                    ]
    +                    for group, offsets in sorted(
    +                        transaction.staged_offsets.items()
    +                    )
    +                },
    +            },
    +            sort_keys=True,
    +            separators=(",", ":"),
    +        ).encode()
    +        crc = zlib.crc32(payload) & 0xFFFFFFFF
    +        with self.path.open("ab") as file:
    +            file.write(f"{crc:08x} ".encode() + payload + b"\n")
    +            file.flush()
    +            os.fsync(file.fileno())
    +
    +    def recover(self) -> dict[str, TransactionData]:
    +        if not self.path.exists():
    +            return {}
    +        transactions: dict[str, TransactionData] = {}
    +        valid_end = 0
    +        with self.path.open("rb") as file:
    +            while line := file.readline():
    +                try:
    +                    crc_text, payload = line.rstrip(b"\n").split(b" ", 1)
    +                    if int(crc_text, 16) != zlib.crc32(payload) & 0xFFFFFFFF:
    +                        break
    +                    raw = json.loads(payload)
    +                    transaction = TransactionData(
    +                        transaction_id=raw["id"],
    +                        state=TransactionState(raw["state"]),
    +                        partitions={
    +                            TopicPartition(topic, partition)
    +                            for topic, partition in raw["partitions"]
    +                        },
    +                        first_offsets={
    +                            TopicPartition(topic, partition): offset
    +                            for topic, partition, offset in raw["first_offsets"]
    +                        },
    +                        staged_offsets={
    +                            group: {
    +                                TopicPartition(topic, partition): offset
    +                                for topic, partition, offset in offsets
    +                            }
    +                            for group, offsets in raw["staged_offsets"].items()
    +                        },
    +                    )
    +                except (ValueError, KeyError, TypeError, json.JSONDecodeError):
    +                    break
    +                transactions[transaction.transaction_id] = transaction
    +                valid_end = file.tell()
    +        if valid_end != self.path.stat().st_size:
    +            with self.path.open("r+b") as file:
    +                file.truncate(valid_end)
    +        return transactions
    ```

??? note "文件差异：src/minikafka/transaction/manager.py"
    ```diff
    diff --git a/src/minikafka/transaction/manager.py b/src/minikafka/transaction/manager.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..c90a1c1c1602a4537a5130b26d1845c009779896
    --- /dev/null
    +++ b/src/minikafka/transaction/manager.py
    @@ -0,0 +1,177 @@
    +from __future__ import annotations
    +
    +import zlib
    +
    +from minikafka.core.batch import ControlType, RecordBatch
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.core.record import Record
    +from minikafka.replication.model import AckMode
    +from minikafka.transaction.journal import TransactionJournal
    +from minikafka.transaction.model import TransactionData, TransactionState
    +
    +
    +class Transaction:
    +    def __init__(
    +        self,
    +        manager: TransactionManager,
    +        data: TransactionData,
    +    ) -> None:
    +        self.manager = manager
    +        self.data = data
    +
    +    async def send(
    +        self,
    +        topic: str,
    +        *,
    +        value: bytes | None,
    +        key: bytes | None = None,
    +        partition: int | None = None,
    +    ) -> None:
    +        await self.manager.send(
    +            self.data,
    +            topic,
    +            value=value,
    +            key=key,
    +            partition=partition,
    +        )
    +
    +    async def send_offsets(
    +        self,
    +        group_id: str,
    +        offsets: dict[TopicPartition, int],
    +    ) -> None:
    +        self.data.staged_offsets.setdefault(group_id, {}).update(offsets)
    +        self.manager.journal.append(self.data)
    +
    +    async def commit(self) -> None:
    +        await self.manager.commit(self.data)
    +
    +    async def abort(self) -> None:
    +        await self.manager.abort(self.data)
    +
    +
    +class TransactionManager:
    +    def __init__(self, cluster: object, journal: TransactionJournal) -> None:
    +        self.cluster = cluster
    +        self.journal = journal
    +        self._transactions = journal.recover()
    +        self._finish_recovery()
    +
    +    async def begin(self, transaction_id: str) -> Transaction:
    +        existing = self._transactions.get(transaction_id)
    +        if existing is not None and existing.state in {
    +            TransactionState.ONGOING,
    +            TransactionState.PREPARE_COMMIT,
    +            TransactionState.PREPARE_ABORT,
    +        }:
    +            raise RuntimeError(f"transaction {transaction_id} is active")
    +        data = TransactionData(transaction_id)
    +        self._transactions[transaction_id] = data
    +        self.journal.append(data)
    +        return Transaction(self, data)
    +
    +    async def send(
    +        self,
    +        data: TransactionData,
    +        topic: str,
    +        *,
    +        value: bytes | None,
    +        key: bytes | None,
    +        partition: int | None,
    +    ) -> None:
    +        self._require_ongoing(data)
    +        metadata = self.cluster.topic(topic)
    +        selected = (
    +            partition
    +            if partition is not None
    +            else (
    +                0
    +                if key is None
    +                else (zlib.crc32(key) & 0xFFFFFFFF) % len(metadata.partitions)
    +            )
    +        )
    +        tp = TopicPartition(topic, selected)
    +        batch = RecordBatch.unassigned(
    +            (Record(key, value, self.cluster.clock.now_ms()),),
    +            transactional_id=data.transaction_id,
    +        )
    +        result = await self.cluster.append_batch(tp, batch, AckMode.LEADER)
    +        data.partitions.add(tp)
    +        if result.base_offset is not None:
    +            data.first_offsets.setdefault(tp, result.base_offset)
    +        self.journal.append(data)
    +
    +    async def commit(self, data: TransactionData) -> None:
    +        self._require_ongoing(data)
    +        data.state = TransactionState.PREPARE_COMMIT
    +        self.journal.append(data)
    +        for tp in sorted(data.partitions):
    +            await self.cluster.append_batch(
    +                tp,
    +                RecordBatch.control_marker(
    +                    transaction_id=data.transaction_id,
    +                    control=ControlType.COMMIT,
    +                ),
    +                AckMode.LEADER,
    +            )
    +        await self.cluster.offsets.commit_many(data.staged_offsets)
    +        data.state = TransactionState.COMPLETE_COMMIT
    +        self.journal.append(data)
    +
    +    async def abort(self, data: TransactionData) -> None:
    +        self._require_ongoing(data)
    +        data.state = TransactionState.PREPARE_ABORT
    +        self.journal.append(data)
    +        for tp in sorted(data.partitions):
    +            await self.cluster.append_batch(
    +                tp,
    +                RecordBatch.control_marker(
    +                    transaction_id=data.transaction_id,
    +                    control=ControlType.ABORT,
    +                ),
    +                AckMode.LEADER,
    +            )
    +        data.staged_offsets.clear()
    +        data.state = TransactionState.COMPLETE_ABORT
    +        self.journal.append(data)
    +
    +    def is_committed(self, transaction_id: str) -> bool:
    +        transaction = self._transactions.get(transaction_id)
    +        return (
    +            transaction is not None
    +            and transaction.state is TransactionState.COMPLETE_COMMIT
    +        )
    +
    +    def last_stable_offset(self, tp: TopicPartition, high_watermark: int) -> int:
    +        unstable = [
    +            transaction.first_offsets[tp]
    +            for transaction in self._transactions.values()
    +            if tp in transaction.first_offsets
    +            and transaction.state
    +            in {
    +                TransactionState.ONGOING,
    +                TransactionState.PREPARE_COMMIT,
    +                TransactionState.PREPARE_ABORT,
    +            }
    +        ]
    +        return min(unstable, default=high_watermark)
    +
    +    @staticmethod
    +    def _require_ongoing(data: TransactionData) -> None:
    +        if data.state is not TransactionState.ONGOING:
    +            raise RuntimeError(
    +                f"transaction {data.transaction_id} is {data.state.value}"
    +            )
    +
    +    def _finish_recovery(self) -> None:
    +        for transaction in self._transactions.values():
    +            if transaction.state is TransactionState.PREPARE_COMMIT:
    +                self.cluster.offsets.commit_many_sync(
    +                    transaction.staged_offsets
    +                )
    +                transaction.state = TransactionState.COMPLETE_COMMIT
    +                self.journal.append(transaction)
    +            elif transaction.state is TransactionState.PREPARE_ABORT:
    +                transaction.staged_offsets.clear()
    +                transaction.state = TransactionState.COMPLETE_ABORT
    +                self.journal.append(transaction)
    ```

??? note "文件差异：src/minikafka/transaction/model.py"
    ```diff
    diff --git a/src/minikafka/transaction/model.py b/src/minikafka/transaction/model.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..5f8d6335a30503d54895fb65b21bfbbf28e2e6d4
    --- /dev/null
    +++ b/src/minikafka/transaction/model.py
    @@ -0,0 +1,25 @@
    +from __future__ import annotations
    +
    +from dataclasses import dataclass, field
    +from enum import Enum
    +
    +from minikafka.core.metadata import TopicPartition
    +
    +
    +class TransactionState(str, Enum):
    +    ONGOING = "ongoing"
    +    PREPARE_COMMIT = "prepare_commit"
    +    COMPLETE_COMMIT = "complete_commit"
    +    PREPARE_ABORT = "prepare_abort"
    +    COMPLETE_ABORT = "complete_abort"
    +
    +
    +@dataclass(slots=True)
    +class TransactionData:
    +    transaction_id: str
    +    state: TransactionState = TransactionState.ONGOING
    +    partitions: set[TopicPartition] = field(default_factory=set)
    +    first_offsets: dict[TopicPartition, int] = field(default_factory=dict)
    +    staged_offsets: dict[str, dict[TopicPartition, int]] = field(
    +        default_factory=dict
    +    )
    ```

**是什么，为什么现在需要**

Transaction 具有 Epoch 与 State；Control Batch 标记 Commit 或 Abort；`read_committed` 过滤未决与已中止数据；Journal 驱动 Prepared Decision 恢复。

**在运行时做什么**

Begin Fencing 旧 Epoch；Send 追加事务 Batch；Prepare 持久记录意图；Commit 写 Marker 与 Offset；Recovery 确定性完成或中止未完成工作。

**关键语句理解**

输出可见性与输入 Offset 发布必须共享同一 Commit Decision，否则恢复会重复输出或跳过输入。

#### 包与工程支撑

保持包导出、依赖与测试环境可复现。

??? note "支撑文件差异（1 个文件）"
    **`src/minikafka/transaction/__init__.py`**

    ```diff
    diff --git a/src/minikafka/transaction/__init__.py b/src/minikafka/transaction/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..41907980d959ff9b2cfb8d0c00a60a133406ef2a
    --- /dev/null
    +++ b/src/minikafka/transaction/__init__.py
    @@ -0,0 +1,4 @@
    +from minikafka.transaction.manager import Transaction, TransactionManager
    +from minikafka.transaction.model import TransactionState
    +
    +__all__ = ["Transaction", "TransactionManager", "TransactionState"]
    ```


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/16-transactions/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

输出可见性与输入 Offset 发布必须共享同一 Commit Decision，否则恢复会重复输出或跳过输入。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 8 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/08-transactions.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/16-transactions/stage.patch)
