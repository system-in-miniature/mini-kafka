# Stage 13 · Acknowledgement modes

### Goal

Build acknowledgement modes and explain its boundary from executable failure, runtime state, and the critical statement.

??? note "Deliverable files"
    - `src/minikafka/errors.py`
    - `src/minikafka/producer/producer.py`
    - `src/minikafka/replication/replica_set.py`
    - `tests/reliability/test_lost_acked_write.py`
    - `tests/replication/test_ack_modes.py`

### The problem at this point

One word such as successful is ambiguous unless the producer knows which durability boundary acknowledged the write.

### Test contract

#### See the failure first

The contract contrasts `acks=0`, `acks=1`, and `acks=all`, then shrinks ISR both before and after append to expose two distinct failure windows.

??? note "File diff: tests/reliability/test_lost_acked_write.py"
    ```diff
    diff --git a/tests/reliability/test_lost_acked_write.py b/tests/reliability/test_lost_acked_write.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..70e1ed79f81b9d0d8c327d593ff77c93db00cc62
    --- /dev/null
    +++ b/tests/reliability/test_lost_acked_write.py
    @@ -0,0 +1,24 @@
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
    +async def test_acks_one_can_confirm_a_leader_only_tail(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 2)
    +        producer = cluster.producer(batch_size=1, acks=1)
    +
    +        acknowledged = await producer.send("events", value=b"at-risk")
    +
    +        tp = TopicPartition("events", 0)
    +        assert acknowledged.offset == 0
    +        assert cluster.replica_log(tp, broker_id=1).leo == 1
    +        assert cluster.replica_log(tp, broker_id=2).leo == 0
    +        assert cluster.visible_end(tp) == 0
    ```

**What this test locks**

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

**How it constructs the counterexample**

The contract contrasts `acks=0`, `acks=1`, and `acks=all`, then shrinks ISR both before and after append to expose two distinct failure windows.

**Key test statement**

```python
assert acknowledged.offset == 0
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

**What a failure means**

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

??? note "File diff: tests/replication/test_ack_modes.py"
    ```diff
    diff --git a/tests/replication/test_ack_modes.py b/tests/replication/test_ack_modes.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..fd1e05a96d19d37f87deed920a0f776d789620b1
    --- /dev/null
    +++ b/tests/replication/test_ack_modes.py
    @@ -0,0 +1,93 @@
    +import asyncio
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.batch import RecordBatch
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.core.record import Record
    +from minikafka.errors import NotEnoughReplicas, NotEnoughReplicasAfterAppend
    +from minikafka.replication.model import AckMode
    +
    +
    +def batch(value: bytes) -> RecordBatch:
    +    return RecordBatch.unassigned((Record(None, value, 0),))
    +
    +
    +@pytest.mark.asyncio
    +async def test_acks_all_waits_for_acknowledgement_set(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(
    +        data_dir=tmp_path,
    +        broker_ids=(1, 2),
    +        min_insync_replicas=2,
    +    )
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 2)
    +        replica_set = cluster.replica_set(TopicPartition("events", 0))
    +
    +        pending = asyncio.create_task(replica_set.append(batch(b"safe"), AckMode.ALL))
    +        await asyncio.sleep(0)
    +        assert not pending.done()
    +
    +        await replica_set.fetch_followers_once()
    +
    +        assert (await pending).next_offset == 1
    +
    +
    +@pytest.mark.asyncio
    +async def test_acks_all_rejects_insufficient_isr_before_append(
    +    tmp_path: Path,
    +) -> None:
    +    config = MiniKafkaConfig(
    +        data_dir=tmp_path,
    +        broker_ids=(1, 2),
    +        min_insync_replicas=2,
    +    )
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 2)
    +        replica_set = cluster.replica_set(TopicPartition("events", 0))
    +        replica_set.remove_from_isr(2)
    +
    +        with pytest.raises(NotEnoughReplicas):
    +            await replica_set.append(batch(b"rejected"), AckMode.ALL)
    +
    +        assert replica_set.leader.leo == 0
    +
    +
    +@pytest.mark.asyncio
    +async def test_acks_all_fails_if_isr_shrinks_after_append(
    +    tmp_path: Path,
    +) -> None:
    +    config = MiniKafkaConfig(
    +        data_dir=tmp_path,
    +        broker_ids=(1, 2),
    +        min_insync_replicas=2,
    +    )
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 2)
    +        replica_set = cluster.replica_set(TopicPartition("events", 0))
    +        pending = asyncio.create_task(replica_set.append(batch(b"x"), AckMode.ALL))
    +        await asyncio.sleep(0)
    +
    +        replica_set.remove_from_isr(2)
    +
    +        with pytest.raises(NotEnoughReplicasAfterAppend):
    +            await pending
    +        assert replica_set.leader.leo == 1
    +
    +
    +@pytest.mark.asyncio
    +async def test_acks_zero_returns_unknown_offsets(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        producer = cluster.producer(batch_size=1, acks=0)
    +
    +        metadata = await producer.send("events", value=b"fire-and-forget")
    +
    +        assert metadata.offset is None
    +        assert metadata.base_offset is None
    +        assert cluster.leader_log(TopicPartition("events", 0)).leo == 1
    ```

**What this test locks**

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

**How it constructs the counterexample**

The contract contrasts `acks=0`, `acks=1`, and `acks=all`, then shrinks ISR both before and after append to expose two distinct failure windows.

**Key test statement**

```python
assert acknowledged.offset == 0
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

**What a failure means**

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

`acks=0` does not return offsets, `acks=1` waits for leader append, and `acks=all` requires the acknowledgement set plus `min.insync.replicas`.

### Why this mechanism is necessary

One word such as successful is ambiguous unless the producer knows which durability boundary acknowledged the write. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

The replica set checks admission before append, tracks required acknowledgers, advances replicas, and fails the waiter if ISR loses a required member before completion.

### Mechanism blocks

#### Acknowledgement modes mechanism

The replica set checks admission before append, tracks required acknowledgers, advances replicas, and fails the waiter if ISR loses a required member before completion.

??? note "File diff: src/minikafka/errors.py"
    ```diff
    diff --git a/src/minikafka/errors.py b/src/minikafka/errors.py
    index e133be2438c4de1cd2d5c862e80d4726a29ae6a0..69b6a1405ea7b1c66e528d2727ed80934b4ee06a 100644
    --- a/src/minikafka/errors.py
    +++ b/src/minikafka/errors.py
    @@ -66,3 +66,11 @@ class NotPartitionOwner(MiniKafkaError):

     class UnknownMember(MiniKafkaError):
         code = "UNKNOWN_MEMBER"
    +
    +
    +class NotEnoughReplicas(MiniKafkaError):
    +    code = "NOT_ENOUGH_REPLICAS"
    +
    +
    +class NotEnoughReplicasAfterAppend(MiniKafkaError):
    +    code = "NOT_ENOUGH_REPLICAS_AFTER_APPEND"
    ```

??? note "File diff: src/minikafka/producer/producer.py"
    ```diff
    diff --git a/src/minikafka/producer/producer.py b/src/minikafka/producer/producer.py
    index bbc391fd217202f2181125fa0057b844294883e3..90892289c9212dba9e8a756d614ac334386b6606 100644
    --- a/src/minikafka/producer/producer.py
    +++ b/src/minikafka/producer/producer.py
    @@ -18,8 +18,8 @@ from minikafka.replication.model import AckMode
     class RecordMetadata:
         topic: str
         partition: int
    -    offset: int
    -    base_offset: int
    +    offset: int | None
    +    base_offset: int | None
         timestamp_ms: int


    @@ -103,16 +103,18 @@ class Producer:
                     if not item.future.done():
                         item.future.set_exception(error)
                 return
    -        if appended.batch.base_offset is None:
    -            raise RuntimeError("leader appended an unassigned batch")
             for delta, item in enumerate(pending):
                 if not item.future.done():
                     item.future.set_result(
                         RecordMetadata(
                             topic=tp.topic,
                             partition=tp.partition,
    -                        offset=appended.batch.base_offset + delta,
    -                        base_offset=appended.batch.base_offset,
    +                        offset=(
    +                            None
    +                            if appended.base_offset is None
    +                            else appended.base_offset + delta
    +                        ),
    +                        base_offset=appended.base_offset,
                             timestamp_ms=item.record.timestamp_ms,
                         )
                     )
    ```

??? note "File diff: src/minikafka/replication/replica_set.py"
    ```diff
    diff --git a/src/minikafka/replication/replica_set.py b/src/minikafka/replication/replica_set.py
    index cc705c9fb62f912ccb5bf1e515917f874e91053c..853087941a0b9541adba5b1e90de4146fb311ec7 100644
    --- a/src/minikafka/replication/replica_set.py
    +++ b/src/minikafka/replication/replica_set.py
    @@ -1,16 +1,29 @@
     from __future__ import annotations

     import asyncio
    +from dataclasses import dataclass

     from minikafka.clock import Clock
     from minikafka.config import MiniKafkaConfig
     from minikafka.core.batch import RecordBatch
     from minikafka.core.metadata import PartitionMetadata, TopicPartition
     from minikafka.core.record import LogRecord
    +from minikafka.errors import (
    +    NotEnoughReplicas,
    +    NotEnoughReplicasAfterAppend,
    +)
     from minikafka.replication.model import AckMode, IsolationLevel, ProduceResult
     from minikafka.replication.replica import Replica


    +@dataclass(slots=True)
    +class AckWaiter:
    +    target_offset: int
    +    acknowledgement_set: frozenset[int]
    +    result: ProduceResult
    +    future: asyncio.Future[ProduceResult]
    +
    +
     class PartitionReplicaSet:
         def __init__(
             self,
    @@ -37,6 +50,7 @@ class PartitionReplicaSet:
                 replica.in_sync = broker_id in self._isr
             self.high_watermark = min(replica.leo for replica in replicas.values())
             self._lock = asyncio.Lock()
    +        self._ack_waiters: list[AckWaiter] = []

         @property
         def leader(self) -> Replica:
    @@ -64,10 +78,19 @@ class PartitionReplicaSet:
             acks: AckMode | str | int = AckMode.LEADER,
         ) -> ProduceResult:
             mode = AckMode.parse(acks)
    +        waiter: AckWaiter | None = None
             async with self._lock:
    +            if (
    +                mode is AckMode.ALL
    +                and len(self._isr) < self.config.min_insync_replicas
    +            ):
    +                raise NotEnoughReplicas(
    +                    f"ISR size {len(self._isr)} is below "
    +                    f"{self.config.min_insync_replicas}"
    +                )
                 located = self.leader.log.append(batch)
                 self._advance_high_watermark()
    -            return ProduceResult(
    +            result = ProduceResult(
                     batch=located.batch,
                     base_offset=(
                         located.batch.base_offset
    @@ -77,6 +100,20 @@ class PartitionReplicaSet:
                     next_offset=located.batch.next_offset,
                     offsets_known=mode is not AckMode.NONE,
                 )
    +            if mode is AckMode.ALL:
    +                waiter = AckWaiter(
    +                    target_offset=located.batch.next_offset,
    +                    acknowledgement_set=frozenset(self._isr),
    +                    result=result,
    +                    future=asyncio.get_running_loop().create_future(),
    +                )
    +                self._ack_waiters.append(waiter)
    +                self._resolve_ack_waiters()
    +            else:
    +                return result
    +        if waiter is None:
    +            raise RuntimeError("acks=all did not create a waiter")
    +        return await waiter.future

         async def fetch_followers_once(self) -> None:
             async with self._lock:
    @@ -122,6 +159,27 @@ class PartitionReplicaSet:
                 self.replicas[broker_id].leo for broker_id in self._isr
             )
             self.high_watermark = max(self.high_watermark, candidate)
    +        self._resolve_ack_waiters()
    +
    +    def _resolve_ack_waiters(self) -> None:
    +        for waiter in self._ack_waiters:
    +            if waiter.future.done():
    +                continue
    +            if len(self._isr) < self.config.min_insync_replicas:
    +                waiter.future.set_exception(
    +                    NotEnoughReplicasAfterAppend(
    +                        "ISR fell below min.insync.replicas after append"
    +                    )
    +                )
    +                continue
    +            if all(
    +                self.replicas[broker_id].leo >= waiter.target_offset
    +                for broker_id in waiter.acknowledgement_set
    +            ):
    +                waiter.future.set_result(waiter.result)
    +        self._ack_waiters = [
    +            waiter for waiter in self._ack_waiters if not waiter.future.done()
    +        ]

         async def fetch(
             self,
    ```

**What it is and why it appears**

`acks=0` does not return offsets, `acks=1` waits for leader append, and `acks=all` requires the acknowledgement set plus `min.insync.replicas`.

**Runtime role**

The replica set checks admission before append, tracks required acknowledgers, advances replicas, and fails the waiter if ISR loses a required member before completion.

**Statement understanding**

Pre-append rejection prevents an under-replicated write; post-append failure prevents a leader-local tail from being mislabeled as fully acknowledged.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/13-ack-modes/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Pre-append rejection prevents an under-replicated write; post-append failure prevents a leader-local tail from being mislabeled as fully acknowledged.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 6](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/06-isr-and-fencing.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/13-ack-modes/stage.patch)
