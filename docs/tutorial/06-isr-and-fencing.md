# Chapter 6 · Replication II: ISR and Fencing

The previous chapter established the physical picture: a leader appends, followers
pull, and the high watermark (HW) bounds ordinary reads. This chapter asks the
harder control-plane questions. Which replicas are safe enough to count toward a
durability promise? When must a write be rejected? How does a new leader make
requests sent to the old leader harmless?

## Learning objectives

By the end of this chapter, you can:

- predict ISR membership from follower time lag, offset lag, and the current HW;
- distinguish the pre-append and post-append failures of `acks=all`;
- explain why `min.insync.replicas` is useful only together with `acks=all`;
- trace promotion, leader-epoch persistence, and stale-request fencing; and
- identify the places where MiniKafka's failover semantics differ from Kafka's.

## The ISR is a promise, not a replica list

Every assigned replica has storage, but only an **in-sync replica** participates
in the current durability boundary. `src/minikafka/replication/replica_set.py`,
`PartitionReplicaSet.__init__`, starts an opened replica set with replicas whose
LEO equals the leader LEO, always adding the leader. It also initializes HW to
the minimum LEO across all replicas. These are startup rules for this teaching
model, not a persisted Kafka controller decision.

The live rule is visible in
`src/minikafka/replication/replica_set.py`,
`PartitionReplicaSet.refresh_isr`:

```python
within_offset = leader_leo - follower.leo <= replica_lag_max_offsets
within_time = now - follower.last_fetch_ms <= replica_lag_time_ms
has_committed_prefix = follower.leo >= self.high_watermark
```

A follower must satisfy all three predicates. The first two say it is neither
too far behind in records nor too old in fetch time. The third is a safety gate:
a replica that does not contain the already committed prefix must not re-enter
ISR, because including its smaller LEO in the HW minimum could contradict what
consumers were previously allowed to see.

The chapter title says “dual condition” because shrink and recovery have
different practical meanings. A current ISR follower is removed when it fails a
lag condition. A removed follower recovers only after a fetch has made it timely
and close **and** it covers HW. In MiniKafka the predicates are recomputed as one
set expression, but reasoning about exit versus safe re-entry prevents a common
mistake: “the follower sent a heartbeat, so it is safe again.” A timely empty
follower is still unsafe.

Follower work is deliberately explicit.
`src/minikafka/replication/replica_set.py`,
`PartitionReplicaSet.fetch_followers_once`, copies complete batches beginning at
each follower's LEO, updates `last_fetch_ms`, and calls `refresh_isr`. There is
no background replication thread. This makes every transition reproducible with
`ManualClock`, but it also means an experiment must call the step that advances
replication.

After membership changes,
`PartitionReplicaSet._advance_high_watermark` computes the minimum LEO of the
current ISR and applies `max(old_hw, candidate)`. Thus HW is exclusive and
monotonic within a running replica-set instance. Ordinary reads in
`src/minikafka/core/cluster.py`, `BrokerCluster.fetch`, stop at
`BrokerCluster.visible_end`, which returns that HW.

## `acks=all` has two failure windows

The write path lives in
`src/minikafka/replication/replica_set.py`,
`PartitionReplicaSet.append`. After checking a supplied leader epoch and
idempotent producer state, it performs a precondition:

```python
if mode is AckMode.ALL and len(self._isr) < min_insync_replicas:
    raise NotEnoughReplicas(...)
```

This is the **pre-append** failure. The leader log has not changed. Retrying
after ISR recovery is straightforward. By contrast, `acks=1` and `acks=0` do
not use this check: `min.insync.replicas` does not magically make those modes
durable.

For `acks=all`, append then captures the current ISR in an `AckWaiter`. The
future completes only when every replica in that captured acknowledgement set
has reached the batch's next offset. Capturing the set is important: a follower
cannot disappear from ISR merely to make the same request easier to satisfy.

There is also a **post-append** failure. In
`PartitionReplicaSet._resolve_ack_waiters`, if ISR falls below
`min_insync_replicas` while the request is waiting, the future fails with
`NotEnoughReplicasAfterAppend`. The record may already exist on the leader.
The client therefore sees an unknown outcome and must retry safely—normally
with idempotence. This is a general distributed-systems lesson: an error
response does not prove that a state change did not happen.

The two errors are defined separately in `src/minikafka/errors.py` as
`NotEnoughReplicas` and `NotEnoughReplicasAfterAppend`. Keeping them separate
makes the ambiguity visible instead of hiding both behind a generic timeout.

## Leader epochs turn old authority into a detectable error

ISR membership limits eligible promotion. In
`src/minikafka/replication/replica_set.py`,
`PartitionReplicaSet.promote`, a broker outside ISR is rejected with
`NotInSyncReplica`. For an accepted candidate, MiniKafka records the current HW
as `safe_end`, truncates the candidate to that boundary if necessary, changes
`leader_id`, increments `leader_epoch`, resets ISR to the new leader, and fails
outstanding acknowledgement waiters with `FencedLeaderEpoch`.

`src/minikafka/core/cluster.py`, `BrokerCluster.promote`, then writes the new
leader ID and epoch through the metadata store. On restart,
`BrokerCluster.open` supplies that epoch to each `PartitionLog`, so the new
authority survives process lifetime. Every appended batch also receives the
current epoch through `RecordBatch.assign` in
`src/minikafka/core/batch.py`.

Finally, `PartitionReplicaSet.append` accepts an optional `leader_epoch`. If a
caller presents the old epoch, the method rejects the request before producer
validation or log append. This is **fencing**: correctness comes from comparing
an authority generation, not from hoping the old leader has stopped running.

MiniKafka does not route the public producer through a metadata refresh loop, so
the explicit epoch argument is primarily a white-box teaching surface. The
invariant is still production-grade: any command whose safety depends on a
leader must carry or derive the leader generation it believes.

## Compared with real Kafka

Read the repository's
[replication and failover mapping](../kafka-mapping.md#replication-and-failover)
and the
[replication evidence rows](../behavior-matrix.md)
before transferring this model to production.

Several distinctions are essential:

- Modern Kafka primarily uses `replica.lag.time.max.ms` for ISR liveness.
  MiniKafka also uses an explicit offset cap, similar to the removed
  `replica.lag.max.messages`, to make deterministic labs possible.
- Kafka controllers and, in current deployments, a KRaft quorum coordinate
  elections and metadata. MiniKafka promotion is an explicit in-process method.
- Kafka brokers are distinct failure domains. MiniKafka stores replicas in
  separate directories but hosts them in one process.
- Kafka uses leader-epoch history for divergence reconciliation (KIP-101 and
  KIP-279). MiniKafka does not.
- Most importantly, MiniKafka's `promote()` truncates the selected leader to
  the old HW. Real Kafka makes the elected leader's LEO the endpoint and aligns
  followers to it. The mapping grades MiniKafka's behavior as **semantically
  opposite** because it can discard data present on all ISR replicas before HW
  advancement. Use it to study a safety boundary, not as a Kafka election
  algorithm.

The `acks=all` waiter is also intentionally smaller than Kafka's replication
machinery. MiniKafka waits for every replica captured at append time; Kafka
completion is integrated with ISR and HW evolution. The shared lesson is the
contract among acknowledgement mode, ISR, and `min.insync.replicas`, not the
exact scheduling algorithm.

This difference also suggests the right way to read a teaching kernel. First,
use the short implementation to make each state transition and rejected input
precise. Then locate the same invariant in the real system and add back
distributed ownership, persisted histories, background scheduling, and
concurrent failures. Reducing the number of participants makes causality
readable; it does not grant permission to remove failure cases from the real
design. In particular, a deterministic promotion call is evidence about this
model's state machine, not evidence that controller election, metadata
propagation, and replica reconciliation are interchangeable steps.

Operationally, monitor ISR size and lag together with acknowledgement errors.
A healthy-looking request rate can conceal a partition that has fallen to one
in-sync copy. `NotEnoughReplicas` is therefore not merely an application error:
it is a signal that the requested durability contract currently has no
admissible execution.

## Hands-on experiment: shrink, recover, and fence

Run this from the repository root. It uses temporary directories and the Direct
API, so it opens no sockets and leaves no data behind.

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run python - <<'PY'
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition
from minikafka.clock import ManualClock
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.errors import FencedLeaderEpoch, NotEnoughReplicas
from minikafka.replication.model import AckMode

async def main():
    with TemporaryDirectory() as d:
        config = MiniKafkaConfig(
            data_dir=Path(d), broker_ids=(1, 2),
            min_insync_replicas=2, replica_lag_max_offsets=0,
        )
        async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
            await cluster.create_topic("events", 1, 2)
            tp = TopicPartition("events", 0)
            rs = cluster.replica_set(tp)
            print("initial ISR:", sorted(rs.isr))
            await rs.append(
                RecordBatch.unassigned((Record(None, b"one", 0),)),
                AckMode.LEADER,
            )
            rs.refresh_isr()
            print("after leader-only append:", sorted(rs.isr))
            try:
                await rs.append(
                    RecordBatch.unassigned((Record(None, b"two", 0),)),
                    AckMode.ALL,
                )
            except NotEnoughReplicas as e:
                print("acks=all precheck:", e.code)
            await rs.fetch_followers_once()
            print("after follower catch-up:", sorted(rs.isr),
                  "HW:", rs.high_watermark)
            old_epoch = rs.leader_epoch
            await cluster.promote(tp, 2)
            print("promoted leader/epoch:", rs.leader_id, rs.leader_epoch)
            try:
                await rs.append(
                    RecordBatch.unassigned((Record(None, b"stale", 0),)),
                    AckMode.LEADER, leader_epoch=old_epoch,
                )
            except FencedLeaderEpoch as e:
                print("stale request:", e.code)

asyncio.run(main())
PY
```

Measured output:

```text
initial ISR: [1, 2]
after leader-only append: [1]
acks=all precheck: NOT_ENOUGH_REPLICAS
after follower catch-up: [1, 2] HW: 1
promoted leader/epoch: 2 1
stale request: FENCED_LEADER_EPOCH
```

The first append creates one offset of lag, which is enough to evict broker 2
because the configured offset allowance is zero. Catch-up restores all three
re-entry predicates. Promotion increments the epoch; presenting epoch zero
afterward is rejected.

## Exercises

1. **Understanding:** Why is “follower LEO is close to leader LEO” insufficient
   for ISR re-entry?

    ??? note "Reference answer"

        It says nothing about the previously committed prefix or fetch
        freshness. MiniKafka therefore requires offset proximity, time
        proximity, and `follower.leo >= high_watermark`. Without the HW gate,
        adding a follower could make the durability boundary contradict data
        already exposed to consumers.

2. **Hands-on:** Copy the experiment to `/tmp/isr_lab.py`, change
   `min_insync_replicas` to `1`, and predict whether the `acks=all` precheck
   still fails. Do not edit `src/`. Acceptance: the script prints an offset for
   the second append instead of `NOT_ENOUGH_REPLICAS`.

    ??? note "Reference answer"

        With ISR `{1}`, its size equals the new minimum, so the precheck passes.
        Because the captured acknowledgement set contains only broker 1, the
        future completes after the leader append. This is legal but provides
        only single-replica durability at that moment.

3. **Hands-on design:** Write a test in `/tmp` that submits `acks=all`, removes
   a follower from ISR after leader append, and asserts
   `NotEnoughReplicasAfterAppend`. Acceptance: running the scratch test with
   `uv run pytest -q /tmp/test_post_append.py` reports `1 passed`.

    ??? note "Reference answer"

        Start `rs.append(..., AckMode.ALL)` with `asyncio.create_task`, yield
        once so the leader append and waiter are created, then call
        `rs.remove_from_isr(2)` while `min_insync_replicas=2`. Await the task
        inside `pytest.raises(NotEnoughReplicasAfterAppend)`. This verifies the
        unknown-outcome branch without changing the implementation.

## Summary

ISR is the set allowed to define the current durability promise, not simply
the replica assignment. MiniKafka exposes safe exit and re-entry, both halves
of `acks=all`, and epoch fencing in a compact state machine—while its HW-based
promotion must not be mistaken for Kafka's leader-epoch reconciliation. With
replica authority established, the next chapter moves up one layer: consumers
must also agree on who owns each partition and which generation may commit.
