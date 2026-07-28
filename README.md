# MiniKafka

MiniKafka is a direct-first reference implementation of Kafka's distinctive
domain semantics. It is a partitioned, replayable event log—not a binary
protocol exercise and not Kafka wire-protocol compatible.

The project concentrates on the mechanisms that make Kafka Kafka:

- append-only partition logs, offsets, segments, sparse indexes, CRC recovery;
- keyed partitioning, batching, consumer positions, replay and lag;
- consumer-group ownership, rebalance generations and stale-member fencing;
- retention, keyed compaction, tombstones and preserved offset gaps;
- leader/follower pull replication, ISR, high watermark and ack modes;
- leader epochs, safe promotion and divergent-tail truncation;
- producer sequence deduplication and epoch fencing;
- transaction markers, read isolation and atomic output-plus-offset commit.

TCP is deliberately a thin adapter. Automatic elections, KRaft and generic
failure detection are deliberately outside the data-plane core.

## Direct API

```python
import asyncio
from pathlib import Path

from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition


async def main() -> None:
    config = MiniKafkaConfig(data_dir=Path("./data"))
    async with BrokerCluster.open(config) as cluster:
        await cluster.create_topic("orders", partitions=3, replication_factor=1)
        producer = cluster.producer(batch_size=1)
        metadata = await producer.send(
            "orders", key=b"user-42", value=b"created"
        )
        records = cluster.fetch(
            TopicPartition("orders", metadata.partition), 0, 100
        )
        print(records)


asyncio.run(main())
```

Core tests invoke `BrokerCluster`, `PartitionLog`, `GroupCoordinator` and
`PartitionReplicaSet` directly. No socket is required to prove domain
correctness.

## Optional JSON/TCP adapter

`JsonTcpServer` accepts bounded newline-delimited JSON. Binary keys and values
use explicit base64 fields. It supports topic administration, metadata,
produce/fetch, group join/heartbeat and offset commits. It exists to prove
transport separation, not to emulate Kafka's evolving wire protocol.

## Reliability experiments

The test suite deterministically demonstrates:

- `acks=1` can acknowledge a leader-only tail that is lost after promotion;
- `acks=all` waits for the captured ISR and respects minimum ISR;
- consumers cannot see records above the high watermark;
- stale leaders and stale group generations are fenced;
- old leaders truncate divergent, uncommitted tails before rejoining;
- retries are deduplicated by producer epoch and sequence;
- aborted and open transactions stay hidden from `read_committed`;
- crash recovery repairs partial batches, indexes and journal tails.

Run:

```bash
uv sync
uv run pytest -q
uv run ruff check .
uv run python -m compileall -q src tests
uv run python tools/count_sloc.py
```

## Scope and simplifications

This is intentionally not a production broker. It uses a custom record format,
manual leader promotion, one-process broker simulation and a bounded
transaction coordinator. It does not implement Kafka wire compatibility,
KRaft/ZooKeeper, TLS/SASL/ACL, quotas, rack-aware placement, tiered storage,
Kafka Connect, Kafka Streams or a production transaction protocol.

The implementation follows concepts documented in Apache Kafka's official
[log implementation](https://kafka.apache.org/43/implementation/log/),
[message format](https://kafka.apache.org/43/implementation/message-format/),
[producer configuration](https://kafka.apache.org/43/configuration/producer-configs/)
and [broker configuration](https://kafka.apache.org/43/configuration/broker-configs/).

## Repository boundary

Course material is separate from this implementation repository. This
repository is the completed, executable reference artifact; a later course
repository may teach it incrementally without coupling lessons to runtime
code or embedding chapter content here.
