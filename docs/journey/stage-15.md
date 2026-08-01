# Stage 15 · Idempotent producer retries

### Goal

Build idempotent producer retries and explain its boundary from executable failure, runtime state, and the critical statement.

??? note "Deliverable files"
    - `src/minikafka/core/cluster.py`
    - `src/minikafka/errors.py`
    - `src/minikafka/producer/producer.py`
    - `src/minikafka/producer/state.py`
    - `src/minikafka/replication/replica_set.py`
    - `tests/producer/test_idempotence.py`
    - `tests/reliability/test_producer_state_restart.py`

### The problem at this point

A lost response makes retry necessary, but a blind retry duplicates a record that may already be durable.

### Test contract

#### See the failure first

Tests resend the exact sequence, create a sequence gap, restart producer state, and start a second instance with the same transactional identity.

??? note "File diff: tests/producer/test_idempotence.py"
    ```diff
    diff --git a/tests/producer/test_idempotence.py b/tests/producer/test_idempotence.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..9769233e796ac8b6db9883ef8139b24a9f2dc5a4
    --- /dev/null
    +++ b/tests/producer/test_idempotence.py
    @@ -0,0 +1,86 @@
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
    +from minikafka.errors import OutOfOrderSequence, ProducerFenced
    +from minikafka.replication.model import AckMode
    +
    +
    +def sequenced(sequence: int, value: bytes, *, epoch: int = 0) -> RecordBatch:
    +    return RecordBatch.unassigned(
    +        (Record(None, value, 0),),
    +        producer_id=7,
    +        producer_epoch=epoch,
    +        base_sequence=sequence,
    +    )
    +
    +
    +@pytest.mark.asyncio
    +async def test_exact_retry_returns_original_offsets(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(
    +        data_dir=tmp_path,
    +        broker_ids=(1, 2),
    +        min_insync_replicas=2,
    +    )
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 2)
    +        replica_set = cluster.replica_set(TopicPartition("events", 0))
    +        batch = sequenced(0, b"x")
    +        pending = asyncio.create_task(replica_set.append(batch, AckMode.ALL))
    +        await asyncio.sleep(0)
    +        await replica_set.fetch_followers_once()
    +        first = await pending
    +
    +        second = await replica_set.append(batch, AckMode.ALL)
    +
    +        assert second == first
    +        assert replica_set.leader.leo == 1
    +
    +
    +@pytest.mark.asyncio
    +async def test_sequence_gap_and_old_epoch_are_rejected(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1,))
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        replica_set = cluster.replica_set(TopicPartition("events", 0))
    +        await replica_set.append(sequenced(0, b"first"), AckMode.LEADER)
    +        with pytest.raises(OutOfOrderSequence):
    +            await replica_set.append(sequenced(2, b"gap"), AckMode.LEADER)
    +        await replica_set.append(
    +            sequenced(0, b"new epoch", epoch=1),
    +            AckMode.LEADER,
    +        )
    +        with pytest.raises(ProducerFenced):
    +            await replica_set.append(
    +                sequenced(1, b"old epoch", epoch=0),
    +                AckMode.LEADER,
    +            )
    +
    +
    +@pytest.mark.asyncio
    +async def test_named_producer_fences_previous_instance(
    +    tmp_path: Path,
    +) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1,))
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        old = cluster.producer(
    +            transactional_name="writer",
    +            idempotent=True,
    +            batch_size=1,
    +        )
    +        cluster.producer(
    +            transactional_name="writer",
    +            idempotent=True,
    +            batch_size=1,
    +        )
    +
    +        with pytest.raises(ProducerFenced):
    +            await old.send("events", value=b"stale")
    ```

**What this test locks**

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

**How it constructs the counterexample**

Tests resend the exact sequence, create a sequence gap, restart producer state, and start a second instance with the same transactional identity.

**Key test statement**

```python
assert second == first
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

**What a failure means**

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

??? note "File diff: tests/reliability/test_producer_state_restart.py"
    ```diff
    diff --git a/tests/reliability/test_producer_state_restart.py b/tests/reliability/test_producer_state_restart.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..b4294702640eb2e6486bed990d541b26b786221e
    --- /dev/null
    +++ b/tests/reliability/test_producer_state_restart.py
    @@ -0,0 +1,40 @@
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
    +from minikafka.replication.model import AckMode
    +
    +
    +def sequenced() -> RecordBatch:
    +    return RecordBatch.unassigned(
    +        (Record(None, b"once", 0),),
    +        producer_id=9,
    +        producer_epoch=0,
    +        base_sequence=0,
    +    )
    +
    +
    +@pytest.mark.asyncio
    +async def test_duplicate_state_rebuilds_from_log(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1,))
    +    tp = TopicPartition("events", 0)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        first = await cluster.replica_set(tp).append(
    +            sequenced(),
    +            AckMode.LEADER,
    +        )
    +
    +    async with BrokerCluster.open(config, clock=ManualClock()) as reopened:
    +        duplicate = await reopened.replica_set(tp).append(
    +            sequenced(),
    +            AckMode.LEADER,
    +        )
    +        assert duplicate.base_offset == first.base_offset
    +        assert reopened.leader_log(tp).leo == 1
    ```

**What this test locks**

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

**How it constructs the counterexample**

Tests resend the exact sequence, create a sequence gap, restart producer state, and start a second instance with the same transactional identity.

**Key test statement**

```python
assert second == first
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

**What a failure means**

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

Producer ID and epoch identify authority; per-partition sequence numbers identify order. An exact retry returns the original offsets, while gaps and old epochs are fenced.

### Why this mechanism is necessary

A lost response makes retry necessary, but a blind retry duplicates a record that may already be durable. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Before append, the state manager compares epoch and sequence with durable per-partition state; after append, it records the batch result for replay across restart.

### Mechanism blocks

#### Idempotent producer retries mechanism

Before append, the state manager compares epoch and sequence with durable per-partition state; after append, it records the batch result for replay across restart.

??? note "File diff: src/minikafka/core/cluster.py"
    ```diff
    diff --git a/src/minikafka/core/cluster.py b/src/minikafka/core/cluster.py
    index 3d3ab7d62bdde8f97946da62dfb933e4fbf991a1..8f84bfe5608505d29093390f80badeb9214d5b5f 100644
    --- a/src/minikafka/core/cluster.py
    +++ b/src/minikafka/core/cluster.py
    @@ -26,6 +26,7 @@ from minikafka.errors import (
     from minikafka.log.partition_log import PartitionLog
     from minikafka.log.segment import LocatedBatch
     from minikafka.producer.producer import Producer
    +from minikafka.producer.state import ProducerIdentityStore
     from minikafka.replication.model import AckMode
     from minikafka.replication.replica import Replica
     from minikafka.replication.replica_set import PartitionReplicaSet
    @@ -56,6 +57,9 @@ class BrokerCluster:
             self._producers: set[Producer] = set()
             self._consumers: set[Consumer] = set()
             self._next_member_id = 1
    +        self._producer_identities = ProducerIdentityStore(
    +            config.data_dir / "producer-identities.json"
    +        )
             self._replica_sets: dict[TopicPartition, PartitionReplicaSet] = {}
             self._build_replica_sets()
             self._closed = False
    @@ -210,8 +214,24 @@ class BrokerCluster:
             linger_ms: int = 0,
             max_buffer_bytes: int = 1_048_576,
             acks: AckMode | str | int = AckMode.LEADER,
    +        idempotent: bool = False,
    +        transactional_name: str | None = None,
         ) -> Producer:
             self._ensure_open()
    +        if idempotent and AckMode.parse(acks) is not AckMode.ALL:
    +            if acks == AckMode.LEADER:
    +                acks = AckMode.ALL
    +            else:
    +                raise ValueError("idempotent producer requires acks=all")
    +        producer_id, producer_epoch = (-1, -1)
    +        if idempotent:
    +            name = transactional_name or f"producer-{len(self._producers) + 1}"
    +            producer_id, producer_epoch = self._producer_identities.allocate(name)
    +            for replica_set in self._replica_sets.values():
    +                replica_set.register_producer_epoch(
    +                    producer_id,
    +                    producer_epoch,
    +                )
             producer = Producer(
                 self,
                 self.clock,
    @@ -219,6 +239,8 @@ class BrokerCluster:
                 linger_ms=linger_ms,
                 max_buffer_bytes=max_buffer_bytes,
                 acks=acks,
    +            producer_id=producer_id,
    +            producer_epoch=producer_epoch,
             )
             self._producers.add(producer)
             return producer
    ```

??? note "File diff: src/minikafka/errors.py"
    ```diff
    diff --git a/src/minikafka/errors.py b/src/minikafka/errors.py
    index 6699db6fca1e5faddcf887581c44582ce75c72e4..6fdc5a3ebf466c84b38018ae62e8644bf001b70d 100644
    --- a/src/minikafka/errors.py
    +++ b/src/minikafka/errors.py
    @@ -82,3 +82,16 @@ class NotInSyncReplica(MiniKafkaError):

     class FencedLeaderEpoch(MiniKafkaError):
         code = "FENCED_LEADER_EPOCH"
    +
    +
    +class ProducerFenced(MiniKafkaError):
    +    code = "PRODUCER_FENCED"
    +
    +
    +class OutOfOrderSequence(MiniKafkaError):
    +    code = "OUT_OF_ORDER_SEQUENCE"
    +
    +    def __init__(self, expected: int, actual: int) -> None:
    +        super().__init__(f"expected sequence {expected}, got {actual}")
    +        self.expected = expected
    +        self.actual = actual
    ```

??? note "File diff: src/minikafka/producer/producer.py"
    ```diff
    diff --git a/src/minikafka/producer/producer.py b/src/minikafka/producer/producer.py
    index 90892289c9212dba9e8a756d614ac334386b6606..7c03cc74f81b8bc8f41907b0f723841e85dbd1e6 100644
    --- a/src/minikafka/producer/producer.py
    +++ b/src/minikafka/producer/producer.py
    @@ -33,11 +33,16 @@ class Producer:
             linger_ms: int,
             max_buffer_bytes: int,
             acks: AckMode | str | int,
    +        producer_id: int = -1,
    +        producer_epoch: int = -1,
         ) -> None:
             self.cluster = cluster
             self.clock = clock
             self.partitioner = Partitioner()
             self.acks = AckMode.parse(acks)
    +        self.producer_id = producer_id
    +        self.producer_epoch = producer_epoch
    +        self._sequences: dict[TopicPartition, int] = {}
             self.accumulator = BatchAccumulator(
                 batch_size=batch_size,
                 linger_ms=linger_ms,
    @@ -95,6 +100,13 @@ class Producer:
                 return
             batch = RecordBatch.unassigned(
                 tuple(item.record for item in pending),
    +            producer_id=self.producer_id,
    +            producer_epoch=self.producer_epoch,
    +            base_sequence=(
    +                self._sequences.get(tp, 0)
    +                if self.producer_id >= 0
    +                else -1
    +            ),
             )
             try:
                 appended = await self.cluster.append_batch(tp, batch, self.acks)
    @@ -103,6 +115,8 @@ class Producer:
                     if not item.future.done():
                         item.future.set_exception(error)
                 return
    +        if self.producer_id >= 0:
    +            self._sequences[tp] = batch.last_sequence + 1
             for delta, item in enumerate(pending):
                 if not item.future.done():
                     item.future.set_result(
    ```

??? note "File diff: src/minikafka/producer/state.py"
    ```diff
    diff --git a/src/minikafka/producer/state.py b/src/minikafka/producer/state.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..a26f1c5e0efbc59025fba0e3ec378dd87d152fca
    --- /dev/null
    +++ b/src/minikafka/producer/state.py
    @@ -0,0 +1,127 @@
    +from __future__ import annotations
    +
    +import json
    +import os
    +from dataclasses import dataclass
    +from pathlib import Path
    +
    +from minikafka.core.batch import RecordBatch
    +from minikafka.errors import OutOfOrderSequence, ProducerFenced
    +from minikafka.replication.model import ProduceResult
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ProducerPartitionState:
    +    epoch: int
    +    last_sequence: int
    +    result: ProduceResult
    +
    +
    +class ProducerStateManager:
    +    def __init__(self, batches: tuple[RecordBatch, ...]) -> None:
    +        self._states: dict[int, ProducerPartitionState] = {}
    +        self._epochs: dict[int, int] = {}
    +        for batch in batches:
    +            if batch.producer_id >= 0:
    +                self.record(batch)
    +
    +    def register_epoch(self, producer_id: int, epoch: int) -> None:
    +        current_epoch = self._epochs.get(producer_id, -1)
    +        if epoch < current_epoch:
    +            raise ProducerFenced(f"producer {producer_id} epoch {epoch}")
    +        self._epochs[producer_id] = epoch
    +        previous = self._states.get(producer_id)
    +        if previous is not None and epoch < previous.epoch:
    +            raise ProducerFenced(f"producer {producer_id} epoch {epoch}")
    +        if previous is not None and epoch > previous.epoch:
    +            self._states[producer_id] = ProducerPartitionState(
    +                epoch,
    +                -1,
    +                previous.result,
    +            )
    +
    +    def validate(self, batch: RecordBatch) -> ProduceResult | None:
    +        if batch.producer_id < 0:
    +            return None
    +        current_epoch = self._epochs.get(batch.producer_id, -1)
    +        if batch.producer_epoch < current_epoch:
    +            raise ProducerFenced(
    +                f"producer {batch.producer_id} epoch "
    +                f"{batch.producer_epoch} is fenced by {current_epoch}"
    +            )
    +        previous = self._states.get(batch.producer_id)
    +        if previous is not None and batch.producer_epoch < previous.epoch:
    +            raise ProducerFenced(
    +                f"producer {batch.producer_id} epoch "
    +                f"{batch.producer_epoch} is fenced by {previous.epoch}"
    +            )
    +        if (
    +            previous is not None
    +            and batch.producer_epoch == previous.epoch
    +            and batch.last_sequence <= previous.last_sequence
    +        ):
    +            recorded = previous.result.batch
    +            if (
    +                batch.base_sequence == recorded.base_sequence
    +                and batch.last_sequence == recorded.last_sequence
    +            ):
    +                return previous.result
    +        expected = (
    +            0
    +            if previous is None or batch.producer_epoch > previous.epoch
    +            else previous.last_sequence + 1
    +        )
    +        if batch.base_sequence != expected:
    +            raise OutOfOrderSequence(expected, batch.base_sequence)
    +        return None
    +
    +    def record(self, batch: RecordBatch) -> ProduceResult:
    +        result = ProduceResult(
    +            batch=batch,
    +            base_offset=batch.base_offset,
    +            next_offset=batch.next_offset,
    +        )
    +        self._states[batch.producer_id] = ProducerPartitionState(
    +            batch.producer_epoch,
    +            batch.last_sequence,
    +            result,
    +        )
    +        self._epochs[batch.producer_id] = max(
    +            self._epochs.get(batch.producer_id, -1),
    +            batch.producer_epoch,
    +        )
    +        return result
    +
    +
    +class ProducerIdentityStore:
    +    def __init__(self, path: Path) -> None:
    +        self.path = path
    +        self._identities = self._load()
    +
    +    def allocate(self, name: str) -> tuple[int, int]:
    +        current = self._identities.get(name)
    +        if current is None:
    +            producer_id, epoch = len(self._identities) + 1, 0
    +        else:
    +            producer_id, epoch = current[0], current[1] + 1
    +        self._identities[name] = (producer_id, epoch)
    +        self._save()
    +        return producer_id, epoch
    +
    +    def _load(self) -> dict[str, tuple[int, int]]:
    +        if not self.path.exists():
    +            return {}
    +        raw = json.loads(self.path.read_text())
    +        return {
    +            name: (int(value[0]), int(value[1]))
    +            for name, value in raw.items()
    +        }
    +
    +    def _save(self) -> None:
    +        self.path.parent.mkdir(parents=True, exist_ok=True)
    +        temporary = self.path.with_suffix(".tmp")
    +        with temporary.open("w") as file:
    +            json.dump(self._identities, file, sort_keys=True)
    +            file.flush()
    +            os.fsync(file.fileno())
    +        os.replace(temporary, self.path)
    ```

??? note "File diff: src/minikafka/replication/replica_set.py"
    ```diff
    diff --git a/src/minikafka/replication/replica_set.py b/src/minikafka/replication/replica_set.py
    index 4d79ee04046084cfb91f878e4ae11eb6470f0b9c..51e1a0002e30d9a7f220918f61ce0477590eaebc 100644
    --- a/src/minikafka/replication/replica_set.py
    +++ b/src/minikafka/replication/replica_set.py
    @@ -14,6 +14,7 @@ from minikafka.errors import (
         NotEnoughReplicasAfterAppend,
         NotInSyncReplica,
     )
    +from minikafka.producer.state import ProducerStateManager
     from minikafka.replication.model import AckMode, IsolationLevel, ProduceResult
     from minikafka.replication.replica import Replica

    @@ -53,6 +54,7 @@ class PartitionReplicaSet:
             self.high_watermark = min(replica.leo for replica in replicas.values())
             self._lock = asyncio.Lock()
             self._ack_waiters: list[AckWaiter] = []
    +        self.producer_state = ProducerStateManager(self.leader.log.all_batches())

         @property
         def leader(self) -> Replica:
    @@ -89,6 +91,9 @@ class PartitionReplicaSet:
                         f"leader epoch {leader_epoch} does not match "
                         f"{self.leader_epoch}"
                     )
    +            duplicate = self.producer_state.validate(batch)
    +            if duplicate is not None:
    +                return duplicate
                 if (
                     mode is AckMode.ALL
                     and len(self._isr) < self.config.min_insync_replicas
    @@ -98,6 +103,7 @@ class PartitionReplicaSet:
                         f"{self.config.min_insync_replicas}"
                     )
                 located = self.leader.log.append(batch)
    +            self.producer_state.record(located.batch)
                 self._advance_high_watermark()
                 result = ProduceResult(
                     batch=located.batch,
    @@ -161,6 +167,9 @@ class PartitionReplicaSet:
                             FencedLeaderEpoch("leader changed before acknowledgement")
                         )
                 self._ack_waiters.clear()
    +            self.producer_state = ProducerStateManager(
    +                self.leader.log.all_batches()
    +            )

         async def rejoin(self, broker_id: int) -> None:
             async with self._lock:
    @@ -174,6 +183,9 @@ class PartitionReplicaSet:
                 replica.in_sync = False
                 self._isr.discard(broker_id)

    +    def register_producer_epoch(self, producer_id: int, epoch: int) -> None:
    +        self.producer_state.register_epoch(producer_id, epoch)
    +
         def refresh_isr(self) -> None:
             now = self.clock.now_ms()
             leader_leo = self.leader.leo
    ```

**What it is and why it appears**

Producer ID and epoch identify authority; per-partition sequence numbers identify order. An exact retry returns the original offsets, while gaps and old epochs are fenced.

**Runtime role**

Before append, the state manager compares epoch and sequence with durable per-partition state; after append, it records the batch result for replay across restart.

**Statement understanding**

Only the next sequence may append, while the immediately repeated sequence may reuse its stored result; these are different branches, not one loose inequality.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/15-idempotent-producer/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Only the next sequence may append, while the immediately repeated sequence may reuse its stored result; these are different branches, not one loose inequality.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 4](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/04-producer.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/15-idempotent-producer/stage.patch)
