# Chapter 1 · Meet MiniKafka

MiniKafka is a small, executable model of Kafka's data-plane ideas. It is not
a tiny production broker and it is not a mock that merely returns convenient
answers. Its logs persist batches, producers choose partitions and batch
records, followers pull from leaders, consumers stop at a high watermark, and
recovery repairs a damaged active tail. The implementation is small enough to
trace in one sitting, but the boundaries are real enough to support meaningful
failure experiments.

## Learning objectives

By the end of this chapter, you will be able to:

1. explain what MiniKafka models and what it deliberately leaves out;
2. create a cluster and topic through the direct API;
3. follow one record from `Producer.send` to `BrokerCluster.fetch`;
4. interpret topic, partition, offset, LEO, and high watermark without
   conflating them; and
5. choose the later chapter that owns each major mechanism.

## Why study an executable teaching kernel?

Reading Apache Kafka itself is indispensable when you need production details,
but it is a difficult first microscope. A single produce request crosses the
network protocol, request dispatch, authorization, quotas, metadata,
replication, storage, metrics, and many compatibility layers. Removing those
layers carelessly produces a toy queue and hides the reason Kafka behaves as it
does.

MiniKafka takes a narrower route. The repository `README.md` describes it as a
“direct-first reference implementation.” “Direct-first” means that tests and
experiments call domain objects in the same Python process. It does **not** mean
that persistence or replication is faked. The thin JSON/TCP adapter is a
separate translation boundary; Chapter 10 discusses it. The direct API lets us
observe domain state deterministically before transport concerns enter the
picture.

The central object is `BrokerCluster` in
`src/minikafka/core/cluster.py`. `BrokerCluster.open` loads metadata, opens one
`PartitionLog` for every assigned replica, restores committed consumer offsets,
and rebuilds replica sets. `BrokerCluster.create_topic` validates the topic
name, calculates round-robin replica assignments, opens the corresponding log
directories, and only then publishes metadata. These are concrete storage and
ownership steps, not an in-memory dictionary pretending to be a broker.

### The first vocabulary

A **topic** is a named collection of partitions. A **partition** is the unit of
ordering and replication. Records within one partition receive monotonically
increasing **offsets**; there is no total order across partitions. A record's
offset is therefore an address inside `(topic, partition)`, not a global message
ID.

The **log end offset** (LEO) is the next offset that a replica would append.
If a partition stores offsets 0, 1, and 2, its LEO is 3. The **high watermark**
(HW) is the end of the replicated prefix that consumers may observe. On a
single-replica MiniKafka partition, append immediately advances HW with LEO. On
a replicated partition, a leader may have an unreplicated tail, so `LEO > HW`.
Chapter 5 makes that gap visible.

The value path starts in `src/minikafka/producer/producer.py`.
`Producer.send` looks up topic metadata, chooses or validates a partition,
creates a `Record`, puts it in `BatchAccumulator`, and returns an
`asyncio.Future`. When a batch becomes ready, `Producer._flush_partition`
creates one `RecordBatch` and calls `BrokerCluster.append_batch`. The return
value is translated into `RecordMetadata` for every pending record.

`BrokerCluster.append_batch` delegates to
`PartitionReplicaSet.append` in
`src/minikafka/replication/replica_set.py`. That function checks producer
sequence state and the requested acknowledgement policy, appends to the
leader's `PartitionLog`, and advances replication-visible state.
`BrokerCluster.fetch` then reads batches from the leader but stops at
`visible_end`, which is the replica set's high watermark. Even the simplest
produce→fetch conversation therefore crosses partitioning, batching, a
replicated-log abstraction, and a visibility boundary.

### Why the context manager matters

The example below uses:

```python
async with BrokerCluster.open(config) as cluster:
    ...
```

`BrokerCluster.__aexit__` calls `BrokerCluster.close`. Closing producers flushes
their accumulators, logs are flushed and closed, and owned background tasks are
joined. This is part of the correctness story. Abandoning the cluster object
would make it unclear whether a pending batch was intentionally lost, still in
memory, or durable. Later failure experiments use the explicit
`BrokerCluster.crash` path when they want different semantics.

## MiniKafka versus Apache Kafka

The correspondence is conceptual, not wire-compatible. A real Kafka client
discovers brokers, serializes a Kafka protocol request, negotiates API versions,
and sends it across a network. MiniKafka's direct adapter skips that transport
work and invokes the same in-process domain core used by its tests.

Real Kafka also distributes brokers across processes or machines and uses a
KRaft controller quorum to manage metadata and elections. MiniKafka stores
metadata in a local JSON file, represents brokers within one process, and makes
promotion explicit. It has no TLS, SASL, ACLs, quotas, rack awareness, tiered
storage, Kafka Connect, or Kafka Streams. Its on-disk record batch is a custom,
uncompressed `MKB1` frame rather than Kafka's wire and log format.

Those omissions are a boundary, not fine print. Consult the
[behavior matrix](../behavior-matrix.md) for executable evidence and the
[MiniKafka → Kafka mapping](../kafka-mapping.md) for graded parity and real
Kafka references. In particular, “direct API works” proves the teaching
kernel's domain path; it does not prove compatibility with `kafka-python`, the
Java producer, or a real Kafka cluster.

The direct route has one further teaching advantage: time, replication rounds,
and failure points remain under the experiment's control. We can stop at the
exact moment after a leader append but before a follower fetch, inspect both
logs, and then advance one mechanism. Doing the same through real sockets adds
operating-system scheduling and network races that obscure the first
explanation. The trade-off is equally important: latency, throughput, request
interleavings, connection loss, and backpressure observed here are not
production measurements. Throughout this book, mechanism evidence and
production equivalence remain separate claims.

## Hands-on experiment: the first record

Run this command from the repository root. The explicit cache directory is
needed in restricted environments where the default user cache is read-only;
it does not change MiniKafka behavior.

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run --offline python - <<'PY'
import asyncio
from tempfile import TemporaryDirectory
from pathlib import Path
from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition

async def main():
    with TemporaryDirectory() as directory:
        async with BrokerCluster.open(MiniKafkaConfig(Path(directory))) as cluster:
            await cluster.create_topic("orders", partitions=2, replication_factor=1)
            producer = cluster.producer(batch_size=1)
            metadata = await producer.send(
                "orders", key=b"alice", value=b"created"
            )
            records = cluster.fetch(
                TopicPartition("orders", metadata.partition), 0, 10
            )
            print(f"partition={metadata.partition} offset={metadata.offset}")
            print([(r.offset, r.key, r.value) for r in records])

asyncio.run(main())
PY
```

Measured output:

```text
partition=1 offset=0
[(0, b'alice', b'created')]
```

The partition is 1 because `Partitioner.choose` in
`src/minikafka/producer/partitioner.py` computes
`zlib.crc32(b"alice") % 2`. The offset is 0 because this is the first append to
that partition. `batch_size=1` causes the pending record to flush immediately.
The returned metadata tells us where the write landed; fetching from a guessed
partition would be an error in a partitioned system.

The temporary directory is important for a tutorial experiment: every run
starts with empty metadata and logs, so the exact output is reproducible. To
study restart instead, replace it with a stable path and open a second cluster
after closing the first.

## Exercises

### 1. Understanding: address versus identity

Why is offset 0 alone insufficient to identify the record in the experiment?
State the smallest complete address represented by the public API.

Acceptance: your answer must name all address components and explain why two
records can both have offset 0.

??? note "Reference answer"

    The complete address is `(topic, partition, offset)`, here
    `("orders", 1, 0)`. Every partition has its own offset sequence, and another
    partition in `orders` can also contain offset 0. A different topic can do
    so as well.

### 2. Hands-on: prove keyed determinism

Change the inline experiment—not repository source—to send three values with
the same key, await all three futures, and print their partitions. Use a large
batch size, call `await producer.flush()`, and verify that all partition numbers
are identical and offsets preserve send order.

Acceptance: the printed partition set has size 1, and the fetched values are
`b"0"`, `b"1"`, `b"2"` in that order. Also run
`git diff -- src` and confirm it prints nothing.

??? note "Reference answer"

    Replace the single send with:

    ```python
    producer = cluster.producer(batch_size=4096, linger_ms=1000)
    pending = [
        producer.send("orders", key=b"alice", value=str(i).encode())
        for i in range(3)
    ]
    await producer.flush()
    metadata = [await future for future in pending]
    print({item.partition for item in metadata})
    ```

    Fetch from `metadata[0].partition`. The set is `{1}` and the values remain
    ordered because one key selects one partition and `_flush_partition`
    preserves accumulator order.

### 3. Code-reading change: expose LEO and HW

Draft, but do not apply, a patch that adds a read-only `describe_offsets(tp)`
helper to `BrokerCluster`. It should return leader LEO and visible end. Name the
existing functions it would delegate to and propose one test.

Acceptance: the draft contains no new stored state, delegates to existing
objects, and the proposed test demonstrates `LEO > HW` before follower fetch on
a two-replica topic.

??? note "Reference answer"

    A minimal design is:

    ```diff
    +def describe_offsets(self, tp: TopicPartition) -> tuple[int, int]:
    +    return self.leader_log(tp).leo, self.visible_end(tp)
    ```

    The test would append with `acks=1`, assert `(1, 0)`, call
    `replica_set.fetch_followers_once()`, then assert `(1, 1)`. No new field is
    necessary because `PartitionLog.leo` and
    `PartitionReplicaSet.high_watermark` already own the state.

## Summary

MiniKafka is an executable semantic model: small enough to trace, but backed by
real partition logs, producer batches, replica state, and restart behavior.
The direct API removes transport noise while retaining those mechanisms. Our
first record acquired a partition and offset, then became visible through the
high-watermark boundary. Chapter 2 now opens that partition log and examines
segments, sparse indexes, CRC frames, and startup recovery—the storage
foundation on which every later guarantee depends.
