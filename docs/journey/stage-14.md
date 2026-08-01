# Stage 14 · Promotion and epoch fencing

### Goal

Build promotion and epoch fencing and explain its boundary from executable failure, runtime state, and the critical statement.

??? note "Deliverable files"
    - `src/minikafka/core/cluster.py`
    - `src/minikafka/errors.py`
    - `src/minikafka/replication/replica_set.py`
    - `tests/reliability/test_lost_acked_write.py`
    - `tests/replication/test_divergent_tail.py`
    - `tests/replication/test_promotion.py`

### The problem at this point

After failover, an old leader may still accept traffic and carry a suffix the new leader never committed.

### Test contract

#### See the failure first

Promotion tests reject non-ISR candidates, increment epochs, issue stale requests, and force the old leader to truncate an uncommitted tail.

??? note "File diff: tests/reliability/test_lost_acked_write.py"
    ```diff
    diff --git a/tests/reliability/test_lost_acked_write.py b/tests/reliability/test_lost_acked_write.py
    index 70e1ed79f81b9d0d8c327d593ff77c93db00cc62..b7d918c02b14d66c63886e2760018e626ba32fe0 100644
    --- a/tests/reliability/test_lost_acked_write.py
    +++ b/tests/reliability/test_lost_acked_write.py
    @@ -22,3 +22,8 @@ async def test_acks_one_can_confirm_a_leader_only_tail(tmp_path: Path) -> None:
             assert cluster.replica_log(tp, broker_id=1).leo == 1
             assert cluster.replica_log(tp, broker_id=2).leo == 0
             assert cluster.visible_end(tp) == 0
    +
    +        await cluster.promote(tp, broker_id=2)
    +
    +        assert cluster.leader_log(tp).leo == 0
    +        assert cluster.fetch(tp, 0, 10) == ()
    ```

**What this test locks**

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

**How it constructs the counterexample**

Promotion tests reject non-ISR candidates, increment epochs, issue stale requests, and force the old leader to truncate an uncommitted tail.

**Key test statement**

```python
assert cluster.leader_log(tp).leo == 0
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

**What a failure means**

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

??? note "File diff: tests/replication/test_divergent_tail.py"
    ```diff
    diff --git a/tests/replication/test_divergent_tail.py b/tests/replication/test_divergent_tail.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..fa67190fd2ae9112242329df781697e8a5bf33a0
    --- /dev/null
    +++ b/tests/replication/test_divergent_tail.py
    @@ -0,0 +1,45 @@
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
    +from minikafka.replication.model import AckMode
    +
    +
    +def batch(value: bytes) -> RecordBatch:
    +    return RecordBatch.unassigned((Record(None, value, 0),))
    +
    +
    +@pytest.mark.asyncio
    +async def test_old_leader_truncates_uncommitted_tail(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(
    +        data_dir=tmp_path,
    +        broker_ids=(1, 2),
    +        min_insync_replicas=2,
    +    )
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 2)
    +        tp = TopicPartition("events", 0)
    +        replica_set = cluster.replica_set(tp)
    +        committed = asyncio.create_task(
    +            replica_set.append(batch(b"committed"), AckMode.ALL)
    +        )
    +        await asyncio.sleep(0)
    +        await replica_set.fetch_followers_once()
    +        await committed
    +        await replica_set.append(batch(b"leader-only"), AckMode.LEADER)
    +
    +        await cluster.promote(tp, broker_id=2)
    +
    +        assert replica_set.high_watermark == 1
    +        assert replica_set.replicas[1].leo == 2
    +        await replica_set.rejoin(1)
    +        assert replica_set.replicas[1].leo == 1
    +        await replica_set.fetch_followers_once()
    +        assert replica_set.replicas[1].leo == replica_set.leader.leo
    ```

**What this test locks**

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

**How it constructs the counterexample**

Promotion tests reject non-ISR candidates, increment epochs, issue stale requests, and force the old leader to truncate an uncommitted tail.

**Key test statement**

```python
assert cluster.leader_log(tp).leo == 0
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

**What a failure means**

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

??? note "File diff: tests/replication/test_promotion.py"
    ```diff
    diff --git a/tests/replication/test_promotion.py b/tests/replication/test_promotion.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..4ff2b19397fcf372f8bda749f6b7375832ae55bf
    --- /dev/null
    +++ b/tests/replication/test_promotion.py
    @@ -0,0 +1,65 @@
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
    +from minikafka.errors import FencedLeaderEpoch, NotInSyncReplica
    +from minikafka.replication.model import AckMode
    +
    +
    +def batch(value: bytes) -> RecordBatch:
    +    return RecordBatch.unassigned((Record(None, value, 0),))
    +
    +
    +@pytest.mark.asyncio
    +async def test_only_isr_replica_can_be_promoted(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 2)
    +        tp = TopicPartition("events", 0)
    +        replica_set = cluster.replica_set(tp)
    +        replica_set.remove_from_isr(2)
    +
    +        with pytest.raises(NotInSyncReplica):
    +            await cluster.promote(tp, broker_id=2)
    +
    +
    +@pytest.mark.asyncio
    +async def test_promotion_increments_epoch_and_fences_old_requests(
    +    tmp_path: Path,
    +) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 2)
    +        tp = TopicPartition("events", 0)
    +
    +        await cluster.promote(tp, broker_id=2)
    +
    +        replica_set = cluster.replica_set(tp)
    +        assert replica_set.leader_id == 2
    +        assert replica_set.leader_epoch == 1
    +        with pytest.raises(FencedLeaderEpoch):
    +            await replica_set.append(
    +                batch(b"stale"),
    +                AckMode.LEADER,
    +                leader_epoch=0,
    +            )
    +
    +
    +@pytest.mark.asyncio
    +async def test_promoted_metadata_survives_restart(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    +    tp = TopicPartition("events", 0)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 2)
    +        await cluster.promote(tp, broker_id=2)
    +
    +    async with BrokerCluster.open(config, clock=ManualClock()) as reopened:
    +        assert reopened.partition_metadata(tp).leader_id == 2
    +        assert reopened.partition_metadata(tp).leader_epoch == 1
    +        assert reopened.replica_set(tp).leader_id == 2
    ```

**What this test locks**

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

**How it constructs the counterexample**

Promotion tests reject non-ISR candidates, increment epochs, issue stale requests, and force the old leader to truncate an uncommitted tail.

**Key test statement**

```python
assert cluster.leader_log(tp).leo == 0
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

**What a failure means**

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

Leader epoch versions authority. Clean promotion chooses an eligible ISR replica. Divergent data after HW is uncommitted and must not survive reconciliation.

### Why this mechanism is necessary

After failover, an old leader may still accept traffic and carry a suffix the new leader never committed. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Promotion validates eligibility, increments and persists the epoch, changes metadata, truncates replicas to the committed boundary, and fences requests carrying older epochs.

### Mechanism blocks

#### Promotion and epoch fencing mechanism

Promotion validates eligibility, increments and persists the epoch, changes metadata, truncates replicas to the committed boundary, and fences requests carrying older epochs.

??? note "File diff: src/minikafka/core/cluster.py"
    ```diff
    diff --git a/src/minikafka/core/cluster.py b/src/minikafka/core/cluster.py
    index 3a35132724a9016de8ef2bbdf6902b21f5200506..3d3ab7d62bdde8f97946da62dfb933e4fbf991a1 100644
    --- a/src/minikafka/core/cluster.py
    +++ b/src/minikafka/core/cluster.py
    @@ -1,5 +1,6 @@
     from __future__ import annotations

    +from dataclasses import replace
     from typing import Self

     from minikafka.clock import Clock, SystemClock
    @@ -285,6 +286,18 @@ class BrokerCluster:
             self.partition_metadata(tp)
             return self._replica_sets[tp]

    +    async def promote(self, tp: TopicPartition, broker_id: int) -> None:
    +        self._ensure_open()
    +        replica_set = self.replica_set(tp)
    +        await replica_set.promote(broker_id)
    +        topic = self._topics[tp.topic]
    +        topic.partitions[tp.partition] = replace(
    +            topic.partitions[tp.partition],
    +            leader_id=replica_set.leader_id,
    +            leader_epoch=replica_set.leader_epoch,
    +        )
    +        self._metadata_store.save(self._topics)
    +
         def _make_replica_set(
             self,
             tp: TopicPartition,
    ```

??? note "File diff: src/minikafka/errors.py"
    ```diff
    diff --git a/src/minikafka/errors.py b/src/minikafka/errors.py
    index 69b6a1405ea7b1c66e528d2727ed80934b4ee06a..6699db6fca1e5faddcf887581c44582ce75c72e4 100644
    --- a/src/minikafka/errors.py
    +++ b/src/minikafka/errors.py
    @@ -74,3 +74,11 @@ class NotEnoughReplicas(MiniKafkaError):

     class NotEnoughReplicasAfterAppend(MiniKafkaError):
         code = "NOT_ENOUGH_REPLICAS_AFTER_APPEND"
    +
    +
    +class NotInSyncReplica(MiniKafkaError):
    +    code = "NOT_IN_SYNC_REPLICA"
    +
    +
    +class FencedLeaderEpoch(MiniKafkaError):
    +    code = "FENCED_LEADER_EPOCH"
    ```

??? note "File diff: src/minikafka/replication/replica_set.py"
    ```diff
    diff --git a/src/minikafka/replication/replica_set.py b/src/minikafka/replication/replica_set.py
    index 853087941a0b9541adba5b1e90de4146fb311ec7..4d79ee04046084cfb91f878e4ae11eb6470f0b9c 100644
    --- a/src/minikafka/replication/replica_set.py
    +++ b/src/minikafka/replication/replica_set.py
    @@ -9,8 +9,10 @@ from minikafka.core.batch import RecordBatch
     from minikafka.core.metadata import PartitionMetadata, TopicPartition
     from minikafka.core.record import LogRecord
     from minikafka.errors import (
    +    FencedLeaderEpoch,
         NotEnoughReplicas,
         NotEnoughReplicasAfterAppend,
    +    NotInSyncReplica,
     )
     from minikafka.replication.model import AckMode, IsolationLevel, ProduceResult
     from minikafka.replication.replica import Replica
    @@ -76,10 +78,17 @@ class PartitionReplicaSet:
             self,
             batch: RecordBatch,
             acks: AckMode | str | int = AckMode.LEADER,
    +        *,
    +        leader_epoch: int | None = None,
         ) -> ProduceResult:
             mode = AckMode.parse(acks)
             waiter: AckWaiter | None = None
             async with self._lock:
    +            if leader_epoch is not None and leader_epoch != self.leader_epoch:
    +                raise FencedLeaderEpoch(
    +                    f"leader epoch {leader_epoch} does not match "
    +                    f"{self.leader_epoch}"
    +                )
                 if (
                     mode is AckMode.ALL
                     and len(self._isr) < self.config.min_insync_replicas
    @@ -118,6 +127,8 @@ class PartitionReplicaSet:
         async def fetch_followers_once(self) -> None:
             async with self._lock:
                 for follower in self.followers:
    +                if follower.leo > self.leader.leo:
    +                    follower.log.truncate_to(self.high_watermark)
                     batches = self.leader.log.read_batches(
                         follower.leo,
                         self.config.replica_fetch_max_bytes,
    @@ -127,6 +138,42 @@ class PartitionReplicaSet:
                     follower.last_fetch_ms = self.clock.now_ms()
                 self.refresh_isr()

    +    async def promote(self, broker_id: int) -> None:
    +        async with self._lock:
    +            if broker_id not in self._isr:
    +                raise NotInSyncReplica(
    +                    f"broker {broker_id} is not in the ISR"
    +                )
    +            safe_end = self.high_watermark
    +            promoted = self.replicas[broker_id]
    +            if promoted.leo > safe_end:
    +                promoted.log.truncate_to(safe_end)
    +            self.leader_id = broker_id
    +            self.leader_epoch += 1
    +            for replica in self.replicas.values():
    +                replica.log.leader_epoch = self.leader_epoch
    +                replica.in_sync = replica.broker_id == broker_id
    +            self._isr = {broker_id}
    +            self.high_watermark = safe_end
    +            for waiter in self._ack_waiters:
    +                if not waiter.future.done():
    +                    waiter.future.set_exception(
    +                        FencedLeaderEpoch("leader changed before acknowledgement")
    +                    )
    +            self._ack_waiters.clear()
    +
    +    async def rejoin(self, broker_id: int) -> None:
    +        async with self._lock:
    +            if broker_id == self.leader_id:
    +                raise ValueError("leader cannot rejoin itself as a follower")
    +            replica = self.replicas[broker_id]
    +            if replica.leo > self.high_watermark:
    +                replica.log.truncate_to(self.high_watermark)
    +            replica.log.leader_epoch = self.leader_epoch
    +            replica.last_fetch_ms = self.clock.now_ms()
    +            replica.in_sync = False
    +            self._isr.discard(broker_id)
    +
         def refresh_isr(self) -> None:
             now = self.clock.now_ms()
             leader_leo = self.leader.leo
    ```

**What it is and why it appears**

Leader epoch versions authority. Clean promotion chooses an eligible ISR replica. Divergent data after HW is uncommitted and must not survive reconciliation.

**Runtime role**

Promotion validates eligibility, increments and persists the epoch, changes metadata, truncates replicas to the committed boundary, and fences requests carrying older epochs.

**Statement understanding**

Comparing the request epoch before mutation turns stale authority into a typed failure; truncating to HW preserves only the agreed prefix.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/14-promotion-fencing/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Comparing the request epoch before mutation turns stale authority into a typed failure; truncating to HW preserves only the agreed prefix.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 6](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/06-isr-and-fencing.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/14-promotion-fencing/stage.patch)
