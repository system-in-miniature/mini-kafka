# Stage 19 · ISR rejoin regression

### Goal

Build isr rejoin regression and explain its boundary from executable failure, runtime state, and the critical statement.

??? note "Deliverable files"
    - `src/minikafka/replication/replica_set.py`
    - `tests/replication/test_promotion.py`

### The problem at this point

A follower can be close to the leader LEO yet still sit behind the committed high watermark, so LEO-only rejoin admits an unsafe election candidate.

### Test contract

#### See the failure first

The regression constructs a replica behind HW, asks it to rejoin, and then attempts promotion; the old predicate would let both operations succeed.

??? note "File diff: tests/replication/test_promotion.py"
    ```diff
    diff --git a/tests/replication/test_promotion.py b/tests/replication/test_promotion.py
    index 4ff2b19397fcf372f8bda749f6b7375832ae55bf..11407d3270a31329ad60b0d41d28e1afca781352 100644
    --- a/tests/replication/test_promotion.py
    +++ b/tests/replication/test_promotion.py
    @@ -29,6 +29,32 @@ async def test_only_isr_replica_can_be_promoted(tmp_path: Path) -> None:
                 await cluster.promote(tp, broker_id=2)


    +@pytest.mark.asyncio
    +async def test_replica_behind_high_watermark_cannot_reenter_isr(
    +    tmp_path: Path,
    +) -> None:
    +    config = MiniKafkaConfig(
    +        data_dir=tmp_path,
    +        broker_ids=(1, 2),
    +        replica_lag_max_offsets=10,
    +    )
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 2)
    +        tp = TopicPartition("events", 0)
    +        replica_set = cluster.replica_set(tp)
    +        await replica_set.append(batch(b"committed"), AckMode.LEADER)
    +        await replica_set.fetch_followers_once()
    +        assert replica_set.high_watermark == 1
    +        replica_set.remove_from_isr(2)
    +        replica_set.replicas[2].log.truncate_to(0)
    +
    +        replica_set.refresh_isr()
    +
    +        assert 2 not in replica_set.isr
    +        with pytest.raises(NotInSyncReplica):
    +            await cluster.promote(tp, broker_id=2)
    +
    +
     @pytest.mark.asyncio
     async def test_promotion_increments_epoch_and_fences_old_requests(
         tmp_path: Path,
    ```

**What this test locks**

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

**How it constructs the counterexample**

The regression constructs a replica behind HW, asks it to rejoin, and then attempts promotion; the old predicate would let both operations succeed.

**Key test statement**

```python
assert replica_set.high_watermark == 1
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

**What a failure means**

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

ISR membership is a safety claim, not a freshness hint. A rejoining replica must contain the entire committed prefix before it can acknowledge or become leader.

### Why this mechanism is necessary

A follower can be close to the leader LEO yet still sit behind the committed high watermark, so LEO-only rejoin admits an unsafe election candidate. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Membership refresh compares follower LEO with both lag policy and HW; only replicas at or beyond HW reenter the set used by acknowledgement and election.

### Mechanism blocks

#### ISR rejoin regression mechanism

Membership refresh compares follower LEO with both lag policy and HW; only replicas at or beyond HW reenter the set used by acknowledgement and election.

??? note "File diff: src/minikafka/replication/replica_set.py"
    ```diff
    diff --git a/src/minikafka/replication/replica_set.py b/src/minikafka/replication/replica_set.py
    index 51e1a0002e30d9a7f220918f61ce0477590eaebc..97c678d2d3e18568bfb96916463e64718d0bf8b6 100644
    --- a/src/minikafka/replication/replica_set.py
    +++ b/src/minikafka/replication/replica_set.py
    @@ -199,7 +199,8 @@ class PartitionReplicaSet:
                     now - follower.last_fetch_ms
                     <= self.config.replica_lag_time_ms
                 )
    -            if within_offset and within_time:
    +            has_committed_prefix = follower.leo >= self.high_watermark
    +            if within_offset and within_time and has_committed_prefix:
                     next_isr.add(follower.broker_id)
             self._isr = next_isr
             for broker_id, replica in self.replicas.items():
    ```

**What it is and why it appears**

ISR membership is a safety claim, not a freshness hint. A rejoining replica must contain the entire committed prefix before it can acknowledge or become leader.

**Runtime role**

Membership refresh compares follower LEO with both lag policy and HW; only replicas at or beyond HW reenter the set used by acknowledgement and election.

**Statement understanding**

The `leo >= high_watermark` conjunct is the missing safety gate: without it, shrinking leader progress can make an incomplete replica appear caught up.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/19-isr-regression/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

The `leo >= high_watermark` conjunct is the missing safety gate: without it, shrinking leader progress can make an incomplete replica appear caught up.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 6](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/06-isr-and-fencing.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/19-isr-regression/stage.patch)
