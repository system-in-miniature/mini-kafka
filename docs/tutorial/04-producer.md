# Chapter 4 · The Producer

A producer does more than hand bytes to a broker. It chooses the partition that
defines ordering, groups records into batches, decides when to flush, tracks
acknowledgements, and—when idempotence is enabled—labels retries so the broker
can recognize duplicates. MiniKafka keeps these decisions in small classes
whose ownership can be traced end to end.

## Learning objectives

After this chapter, you will be able to:

1. predict keyed and keyless partition choices;
2. explain how `batch_size`, `linger_ms`, and the buffer limit interact;
3. trace per-record futures through a batch flush;
4. derive idempotent retry, sequence-gap, and epoch-fencing outcomes; and
5. state precisely how MiniKafka's producer differs from Kafka's producer.

## Partitioning defines the ordering domain

`Producer.send` in `src/minikafka/producer/producer.py` reads topic metadata and
chooses a partition unless the caller supplies one explicitly. It delegates to
`Partitioner.choose` in `src/minikafka/producer/partitioner.py`.

For a non-null key, the rule is:

```python
zlib.crc32(key) % partition_count
```

The same byte key and partition count therefore select the same partition.
That gives all records for one key a partition-local order. It does not promise
an order across keys, and changing the topic's partition count can change the
mapping.

For a null key, the partitioner is sticky. It remembers one partition for each
partition count and keeps sending there until a batch containing a keyless
record closes. `Partitioner.on_batch_closed` then rotates to the next partition.
Sticky selection allows adjacent keyless records to share a batch instead of
round-robining every record into tiny batches.

This hash is **not Kafka-client compatible**. Kafka's default producer uses its
own partitioning algorithm and has evolved its sticky behavior. MiniKafka's
CRC32 rule is deterministic and pedagogical; matching it does not predict the
partition selected by an arbitrary real Kafka client.

## Accumulation: size, time, and bounded memory

`Producer.send` creates a `Record`, an `asyncio.Future`, and a
`PendingRecord`. Its `estimated_bytes` is a simple estimate:

```python
39 + len(key or b"") + len(value or b"")
```

`BatchAccumulator.add` in `src/minikafka/producer/accumulator.py` rejects the
record with `ProducerBufferFull` if adding it would exceed
`max_buffer_bytes`. Otherwise it stores the record in a list keyed by
`TopicPartition`, records the first enqueue time, and returns whether that
partition's estimated bytes reached `batch_size`.

When size is reached, `Producer._schedule_flush` starts an asynchronous
`_flush_partition`. When time is reached,
`BatchAccumulator.due_partitions` identifies partitions whose oldest pending
record has waited at least `linger_ms`; `Producer.run_due_flushes` flushes them.
`Producer.flush` ignores both thresholds and drains every partition plus any
already scheduled tasks.

`linger_ms` is deterministic only when something drives the flush check.
MiniKafka does not run a permanent sender thread. Tests use a `ManualClock`,
advance it, and call `run_due_flushes`. This makes the boundary easy to observe
without sleeping, but it is not a model of Kafka's continuously running network
sender.

### One batch, many futures

`Producer._flush_partition` atomically pops the pending list for one partition.
It creates a single `RecordBatch` preserving list order. For an idempotent
producer it attaches the producer ID, epoch, and current base sequence;
otherwise those fields remain `-1`.

The cluster append returns a batch-level `ProduceResult`. The producer then
completes each record's future with `RecordMetadata`, deriving an individual
offset as `base_offset + delta`. With `acks=0`, the result deliberately has no
known base offset, so every returned metadata offset is `None`, even though the
teaching broker has appended locally.

Only after a successful idempotent append does `_flush_partition` advance the
next sequence for that topic-partition. Only after a keyless batch succeeds
does it rotate the sticky partition. If append raises, the same exception is
forwarded to every pending future in the batch.

## Broker-side idempotence

Retries become dangerous when the producer cannot tell whether an earlier
request succeeded. Sending the payload again without identity can append it
twice. MiniKafka models Kafka's PID/epoch/sequence idea in
`ProducerStateManager` (`src/minikafka/producer/state.py`).

The state is per partition replica set and keyed by producer ID. A
`ProducerPartitionState` stores:

- the accepted producer epoch;
- the last sequence in the most recent batch; and
- the original `ProduceResult`.

`ProducerStateManager.validate` first fences a batch whose epoch is older than
the registered or recorded epoch. For the same epoch, if the candidate's
sequence range exactly matches the most recently accepted batch, it returns the
original result instead of appending. The retry receives the original offset.

Otherwise the expected base sequence is zero for a new epoch, or the previous
last sequence plus one. A gap, overlap, or older retry raises
`OutOfOrderSequence`. After append,
`ProducerStateManager.record` stores the assigned batch and result.

`BrokerCluster.producer(idempotent=True)` in
`src/minikafka/core/cluster.py` allocates a stable producer ID and incrementing
epoch through `ProducerIdentityStore.allocate`. The identity file is written to
a temporary JSON file, flushed, fsynced, and atomically replaced. A second
producer with the same `transactional_name` increments the epoch and fences the
old instance. MiniKafka also forces idempotent producers to use `acks=all`;
an explicitly incompatible acknowledgement mode is rejected.

### Rebuilding state after restart

`PartitionReplicaSet.__init__` constructs `ProducerStateManager` from
`leader.log.all_batches()`. Its constructor calls `record` for stored batches
that have a non-negative producer ID. Exact-retry recognition therefore
survives a clean restart without a separate producer-state checkpoint.

This reconstruction relies on the log retained at startup. It is intentionally
small and scans all batches. Production Kafka maintains richer snapshots and
state to avoid unbounded replay.

## MiniKafka versus Apache Kafka

Kafka's producer has serializers, metadata refresh, request-size limits,
compression, delivery timeouts, retries, backoff, multiple in-flight requests,
and a network sender. MiniKafka uses bytes directly, uncompressed batches, an
estimated buffer size, explicit flush driving, and in-process append.

The largest idempotence difference is the duplicate window. Kafka retains the
latest five batches per producer-partition, consistent with its supported
in-flight request window. MiniKafka remembers only the latest exact batch. An
older reordered retry that Kafka might deduplicate becomes
`OutOfOrderSequence` here. MiniKafka's transaction mechanism is also not built
on this producer sequence state, so Chapter 8 must not call it Kafka's complete
zombie-fenced transaction protocol.

See the [behavior matrix](../behavior-matrix.md) for tests proving keyed/sticky
partitioning, batching, exact retry, and restart rebuild. The
[Kafka mapping](../kafka-mapping.md) grades these mechanisms and links KIP-98.

## Hands-on experiment: batching and exact retry

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run --offline python - <<'PY'
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.replication.model import AckMode

async def main():
    with TemporaryDirectory() as directory:
        config = MiniKafkaConfig(Path(directory))
        tp = TopicPartition("events", 0)
        retry = RecordBatch.unassigned(
            (Record(b"id", b"once", 0),),
            producer_id=77, producer_epoch=0, base_sequence=0,
        )
        async with BrokerCluster.open(config) as cluster:
            await cluster.create_topic("events", 2, 1)
            producer = cluster.producer(batch_size=4096, linger_ms=1000)
            pending = [
                producer.send(
                    "events", key=b"same", value=str(i).encode()
                )
                for i in range(3)
            ]
            await producer.flush()
            metadata = [await item for item in pending]
            print("keyed partitions:", [m.partition for m in metadata])
            print(
                "batches in keyed partition:",
                cluster.debug_batch_count(
                    TopicPartition("events", metadata[0].partition)
                ),
            )
            first = await cluster.replica_set(tp).append(retry, AckMode.ALL)
            duplicate = await cluster.replica_set(tp).append(retry, AckMode.ALL)
            print(
                "exact retry offsets:", first.base_offset,
                duplicate.base_offset, "leo:", cluster.leader_log(tp).leo
            )
        async with BrokerCluster.open(config) as reopened:
            recovered = await reopened.replica_set(tp).append(
                retry, AckMode.ALL
            )
            print(
                "restart retry offset:", recovered.base_offset,
                "leo:", reopened.leader_log(tp).leo
            )

asyncio.run(main())
PY
```

Measured output:

```text
keyed partitions: [0, 0, 0]
batches in keyed partition: 1
exact retry offsets: 3 3 leo: 4
restart retry offset: 3 leo: 4
```

All keyed records choose partition 0 and `flush` forms one batch. They occupy
offsets 0–2 there. The manually sequenced batch is assigned offset 3. Its exact
retry returns 3 while LEO stays 4, proving there was no second append. After
restart the rebuilt producer state recognizes the same retry and LEO remains
unchanged.

## Exercises

### 1. Understanding: size versus linger

A partition holds one pending record below `batch_size`. The manual clock has
not reached `linger_ms`. What three public actions can nevertheless cause it to
flush in this implementation?

Acceptance: distinguish adding bytes, advancing time, and explicitly draining.

??? note "Reference answer"

    A later `send` can make accumulated estimated bytes reach `batch_size` and
    schedule a flush. Advancing the clock past `linger_ms` **and then calling**
    `run_due_flushes` flushes it. Calling `producer.flush` or `producer.close`
    drains it regardless of size or time. Merely advancing `ManualClock` starts
    no background sender.

### 2. Hands-on: observe sticky rotation

Create a three-partition topic and a producer with `batch_size=1`. Send three
keyless records sequentially and print their partitions.

Acceptance: the partitions rotate `0, 1, 2`; each future completes; and
`git diff -- src` remains empty.

??? note "Reference answer"

    Use:

    ```python
    producer = cluster.producer(batch_size=1, linger_ms=1000)
    metadata = [
        await producer.send("events", value=value)
        for value in (b"a", b"b", b"c")
    ]
    print([item.partition for item in metadata])
    ```

    Each one-record batch closes immediately. `_flush_partition` calls
    `Partitioner.on_batch_closed`, rotating the sticky choice after successful
    append.

### 3. Code-change design: expand the duplicate window

Draft, without applying, a change that retains five recent results per producer
ID and epoch instead of one. Include the data model, validation rule, rebuild
behavior, and tests.

Acceptance: exact old retries return their own original offsets, unseen
overlaps still fail, epoch fencing remains intact, and memory is bounded.

??? note "Reference answer"

    Replace the single result in `ProducerPartitionState` with a bounded deque
    of five `(base_sequence, last_sequence, ProduceResult)` entries plus the
    latest sequence. `validate` searches that deque for an exact range before
    enforcing `latest + 1`. `record` appends and evicts the oldest entry.
    Reconstruction naturally replays log batches into the deque, but tests must
    cover six batches, retry of each retained batch, rejection of the evicted
    oldest retry, restart, and a higher epoch resetting sequence zero while
    fencing the old epoch.

## Summary

The producer decides the ordering domain before storage sees a record. Keys
select stable partitions; keyless records stick long enough to batch.
Accumulator size, linger, and buffer bounds control when records become one
`RecordBatch`, while futures preserve per-record outcomes. PID, epoch, and
sequence let the broker return an original result for an exact retry and reject
ambiguous order. Chapter 5 now follows those batches beyond the leader: follower
pulls, LEO, high watermark, and `acks=0/1/all` turn an append into a durability
choice.
