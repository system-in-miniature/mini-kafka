# Chapter 5 · Replication I: Followers and Watermarks

Replication separates “the leader accepted this batch” from “the replicated
log may safely expose this batch.” MiniKafka models that separation with one
`PartitionReplicaSet` per topic-partition. Followers pull complete batches from
their own LEO, the high watermark marks the common committed prefix, and the
producer's acknowledgement mode decides how much evidence it waits for.

## Learning objectives

After this chapter, you will be able to:

1. trace one follower-fetch round from follower LEO to appended batches;
2. distinguish each replica's LEO from the partition high watermark;
3. explain why consumers cannot read a leader-only tail;
4. predict the results and risks of `acks=0`, `acks=1`, and `acks=all`; and
5. demonstrate how an acknowledged `acks=1` write can be lost on promotion.

## Replica-set state

`BrokerCluster.create_topic` in `src/minikafka/core/cluster.py` assigns replica
broker IDs for every partition and opens one `PartitionLog` per assignment.
`BrokerCluster._make_replica_set` wraps those logs in `Replica` objects and
constructs `PartitionReplicaSet` from
`src/minikafka/replication/replica_set.py`.

Each `Replica` exposes its LEO through `Replica.leo`, which delegates to the
underlying `PartitionLog.leo`. The replica set owns:

- `leader_id` and `leader_epoch`;
- the replica map;
- the in-sync replica set (ISR);
- `high_watermark`;
- pending `acks=all` waiters; and
- broker-side producer sequence state.

At construction, replicas whose LEO equals the leader's LEO enter the ISR, and
the leader is always added. The initial high watermark is the minimum LEO over
all assigned replicas. Restart reconstruction is intentionally simpler than
Kafka: MiniKafka derives HW from current replica logs rather than loading
replication checkpoint files.

## Followers pull; leaders do not push

`PartitionReplicaSet.fetch_followers_once` is one deterministic replication
round. Under the replica-set lock, it loops through followers. If a follower is
ahead of the leader, it first truncates that divergent suffix to the current
high watermark. It then calls:

```python
leader.log.read_batches(
    follower.leo,
    config.replica_fetch_max_bytes,
)
```

Each returned batch already has an assigned base offset and leader epoch.
`follower.log.append_replica_batch` requires the batch base to equal follower
LEO, preserving continuity. The follower's `last_fetch_ms` is updated, and
`refresh_isr` reevaluates membership and the watermark.

This method copies complete batches, not individual records. `read_batches`
will return one first batch even when it exceeds the byte budget, then stops
before adding a later batch that would cross the budget. That guarantees
progress without splitting a CRC-protected frame.

There is no automatic network fetcher in MiniKafka. A lab or test explicitly
calls `fetch_followers_once`, or `BrokerCluster.replicate_all_once` pumps every
partition. This deterministic control is why we can pause between leader append
and follower replication and inspect the unsafe tail.

## LEO, ISR, and high watermark

LEO is local: it is where one replica will append next. HW is shared
visibility: it is the greatest prefix that the replica set has declared
committed. `PartitionReplicaSet._advance_high_watermark` computes the minimum
LEO among current ISR members and then takes:

```python
self.high_watermark = max(self.high_watermark, candidate)
```

The `max` makes HW monotonic during a leader epoch. A temporarily lagging or
newly evaluated member must not move the committed boundary backwards.

`refresh_isr` requires a follower to satisfy three conditions:

1. its offset lag is within `replica_lag_max_offsets`;
2. its last fetch time is within `replica_lag_time_ms`; and
3. its LEO covers the current high watermark.

The third condition prevents re-entry from shrinking the committed prefix.
MiniKafka's explicit offset-lag threshold is a teaching control resembling
Kafka's removed `replica.lag.max.messages`; current Kafka primarily uses time
lag. Chapter 6 studies ISR removal, re-entry, minimum ISR, and epochs in detail.

### Visibility stops at HW

`PartitionReplicaSet.fetch` passes `end_offset=self.high_watermark` to
`PartitionLog.fetch` for `read_uncommitted`. Despite that name, it still cannot
cross HW; “uncommitted” in Chapter 8 refers to transaction isolation, not
unreplicated bytes.

`BrokerCluster.fetch` likewise sets `end_offset` from
`BrokerCluster.visible_end`, which returns replica-set HW. The consumer path
therefore cannot observe a leader-only record. Once a follower catches up and
HW advances, the same offset becomes readable without changing the record.

## What acknowledgements promise

`AckMode.parse` in `src/minikafka/replication/model.py` accepts `0`, `1`,
`all`, and maps Kafka's `-1` spelling to `all`.
`PartitionReplicaSet.append` implements the modes after validating producer
state.

### `acks=0`: no offset knowledge

The leader still appends in this in-process model, but `ProduceResult.base_offset`
is `None` and `offsets_known` is false. The producer future completes with
`RecordMetadata.offset=None`. This models fire-and-forget response semantics,
not a realistic network send buffer or silent broker failure.

### `acks=1`: leader-local acknowledgement

The leader appends and returns its assigned offset without waiting for
followers. HW may remain unchanged. If the leader fails before replication,
promotion of a follower at HW discards the acknowledged tail. The measured
experiment below demonstrates exactly that loss.

### `acks=all`: wait for the captured ISR

Before append, the ISR size must meet `min_insync_replicas`, or
`NotEnoughReplicas` is raised without writing. After leader append, an
`AckWaiter` captures the current ISR as its `acknowledgement_set` and targets
the batch's next offset. Its future completes only when every captured member's
LEO reaches that target.

If ISR falls below `min_insync_replicas` while waiting,
`_resolve_ack_waiters` raises `NotEnoughReplicasAfterAppend`. Notice the
important ambiguity: the batch is already on the leader even though the client
gets an error. A retry needs idempotence to avoid duplication.

`acks=all` is not “all configured replicas forever.” It means the ISR captured
for this append, subject to minimum ISR. Chapter 6 explores why ISR quality and
leader-election policy determine the strength behind that phrase.

## MiniKafka versus Apache Kafka

Real Kafka followers also fetch from leaders, leaders track replica progress,
HW gates consumer visibility, and producer `acks` combines with
`min.insync.replicas`. Those are the core correspondences.

MiniKafka places all replicas in one process and advances replication only when
the caller pumps it. It has no broker-to-broker network, fetch sessions,
throttling, controller-driven automatic election, rack placement, or
replication checkpoint files. Promotion is manual. Most importantly, its
failover truncates at HW and may truncate the **promoted** replica. Kafka's
leader-epoch reconciliation from KIP-101 treats the elected leader as the
source of truth and truncates followers to it; MiniKafka's direction is
semantically opposite in that edge.

The [behavior matrix](../behavior-matrix.md) points to follower-fetch,
high-watermark, acknowledgement, and acknowledged-loss tests. The
[Kafka mapping](../kafka-mapping.md) records the startup and promotion
differences. Do not infer production failover safety merely because
`acks=all` completes in this deterministic model.

## Hands-on experiment: safe prefix and lost tail

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run --offline python - <<'PY'
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition

async def main():
    with TemporaryDirectory() as directory:
        config = MiniKafkaConfig(
            Path(directory), broker_ids=(1, 2), min_insync_replicas=2
        )
        async with BrokerCluster.open(config) as cluster:
            await cluster.create_topic("events", 1, 2)
            tp = TopicPartition("events", 0)
            replica_set = cluster.replica_set(tp)
            safe_producer = cluster.producer(batch_size=1, acks="all")
            pending = safe_producer.send("events", value=b"safe")
            await asyncio.sleep(0)
            print(
                "acks=all pending:", not pending.done(),
                "HW:", replica_set.high_watermark
            )
            await replica_set.fetch_followers_once()
            safe = await pending
            print(
                "acks=all offset:", safe.offset,
                "LEOs:", [replica_set.replicas[i].leo for i in (1, 2)],
                "HW:", replica_set.high_watermark
            )

            risky_producer = cluster.producer(batch_size=1, acks=1)
            risky = await risky_producer.send(
                "events", value=b"at-risk"
            )
            print(
                "acks=1 offset:", risky.offset,
                "visible end:", cluster.visible_end(tp)
            )
            await cluster.promote(tp, 2)
            print(
                "after promotion:",
                [r.value for r in cluster.fetch(tp, 0, 10)]
            )

asyncio.run(main())
PY
```

Measured output:

```text
acks=all pending: True HW: 0
acks=all offset: 0 LEOs: [1, 1] HW: 1
acks=1 offset: 1 visible end: 1
after promotion: [b'safe']
```

The first send is pending after the leader append because follower LEO and HW
are still zero. One follower-fetch round copies the batch; both LEOs and HW
become one, completing the waiter.

The second send receives offset 1 from the leader, but visible end stays at 1:
offset 1 is just beyond the readable prefix. Broker 2 is then promoted at safe
end 1. Its log contains only `safe`, so the acknowledged `at-risk` tail is
absent. This is not random failure simulation; it is a controlled statement of
what `acks=1` did and did not promise.

## Exercises

### 1. Understanding: two meanings of “commit”

Why can `read_uncommitted` not read above HW? How does this differ from
transactional commit?

Acceptance: identify the replication boundary and the transaction-isolation
boundary separately.

??? note "Reference answer"

    HW is the replicated-prefix visibility boundary for every consumer, so
    neither isolation level reads beyond it. `read_uncommitted` may include
    transactional data whose commit/abort outcome is unresolved or aborted;
    `read_committed` additionally stops at the last stable offset and filters
    aborted transactions. “Uncommitted” does not mean “leader-only.”

### 2. Hands-on: inspect `acks=0`

In a single-replica cluster, create a producer with `acks=0`, send one value,
and print metadata offsets, leader LEO, and fetched data.

Acceptance: metadata offset and base offset are `None`, LEO is 1, the record is
fetchable, and `git diff -- src` is empty.

??? note "Reference answer"

    The core lines are:

    ```python
    producer = cluster.producer(batch_size=1, acks=0)
    metadata = await producer.send("events", value=b"fire-and-forget")
    print(metadata.offset, metadata.base_offset)
    print(cluster.leader_log(tp).leo)
    print([r.value for r in cluster.fetch(tp, 0, 10)])
    ```

    Output is `None None`, `1`, and `[b'fire-and-forget']`. This proves only the
    in-process append path; a real `acks=0` client does not receive a broker
    produce response and can miss transport or broker errors.

### 3. Code-change design: automatic follower loop

Draft, without applying, a lifecycle design that periodically calls
`fetch_followers_once`. Cover shutdown, crash, failure propagation, and
deterministic tests.

Acceptance: the loop is owned and joinable, does not hide terminal exceptions,
and tests do not depend on wall-clock sleeps.

??? note "Reference answer"

    Add a cluster-owned task per replica set or one scheduler task that waits on
    an injectable clock/tick source, calls follower fetch, and records terminal
    failure through the existing lifecycle/failure-injector boundary.
    `close` should request stop, await the task, then flush logs; `crash` should
    cancel without graceful draining. Tests should drive a manual tick, assert
    LEO/HW transitions, inject a fetch error, and assert later public operations
    surface the terminal error. A free-running `asyncio.sleep` loop would make
    the current deterministic experiments flaky.

## Summary

Followers copy complete batches from their own LEO, while HW marks the
replicated prefix visible to consumers. `acks=0` returns no offset knowledge,
`acks=1` confirms only the leader append, and `acks=all` waits for a captured
ISR subject to minimum ISR. The lost-tail experiment turns those words into
observable state. Chapter 6 continues the same mechanism at its hardest
boundary: ISR shrink and recovery, minimum-ISR failures, leader epochs,
promotion, and fencing.
