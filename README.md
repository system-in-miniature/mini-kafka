> **Language**: English | [简体中文](README.zh-CN.md)

# MiniKafka

[![CI](https://github.com/system-in-miniature/mini-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/system-in-miniature/mini-kafka/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) ![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-blue.svg)

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

## Learning modes

- **Mechanism Tutorial** — study the concepts and runtime paths through the
  [bilingual ten-chapter tutorial](docs/tutorial/index.md).
- **Self-Guided Rebuild** — rebuild MiniKafka through twenty independently
  browsable Stages with failure evidence and grouped diffs in the
  [Journey](docs/journey/index.md).
- **Agent-Guided Rebuild** — ask Codex to guide, implement, explain, and verify
  one Stage interactively; see the short [CLI usage guide](docs/agent-guide.md).

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

Two reader-facing labs turn the most important trade-offs into narrated
experiments:

```bash
uv run python -m minikafka.labs.leader_failure
uv run python -m minikafka.labs.rebalance
```

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

Important semantic differences are explicit:

- idempotent-producer deduplication remembers one batch per producer/partition,
  while Kafka retains the latest five; an older out-of-order retry is rejected
  instead of deduplicated;
- high watermarks are reconstructed from replica LEOs at startup rather than
  loaded from Kafka-style replication checkpoint files;
- failover truncates at the high watermark instead of using the leader-epoch
  divergence protocol from KIP-101, and MiniKafka may truncate the promoted
  replica itself—behavior that is semantically opposite to Kafka;
- `PartitionLog.truncate_to` rewrites the retained log into a new segment and
  startup scans every batch to recover the maximum leader epoch; Kafka
  truncates tail files and uses a leader-epoch checkpoint;
- record batches are always uncompressed. There is no gzip or other
  compression implementation;
- group rebalance is an immediate coordinator-side assignment, not Kafka's
  two-phase JoinGroup/SyncGroup protocol, and has no revoke barrier or
  cooperative rebalance;
- committed offsets live in an atomically replaced JSON file, not Kafka's
  compacted `__consumer_offsets` topic;
- transactions now use `acks=all` for data and markers, but they are not built
  on the idempotent producer's PID/epoch sequence state, so this is not Kafka's
  zombie-fenced transaction protocol;
- retention deletes only a continuous prefix of closed segments, matching
  Kafka's no-middle-hole invariant; timestamp indexing and `offsetsForTimes`
  remain unimplemented;
- compaction rebuilds a sibling directory and installs it with two atomic
  renames. The pair is not one atomic exchange, although in-process failure
  rolls back; this is a teaching implementation rather than Kafka's
  background log-cleaner file lifecycle.

See [MiniKafka → Kafka mapping](docs/kafka-mapping.md) for the complete,
graded comparison and references to relevant Kafka configurations and KIPs.

The implementation follows concepts documented in Apache Kafka's official
[log implementation](https://kafka.apache.org/43/implementation/log/),
[message format](https://kafka.apache.org/43/implementation/message-format/),
[producer configuration](https://kafka.apache.org/43/configuration/producer-configs/)
and [broker configuration](https://kafka.apache.org/43/configuration/broker-configs/).

## Repository boundary

The reference implementation, mechanism tutorial, and reconstruction Journey
live together so every learning claim can point to executable evidence. The
Journey owns teaching artifacts only; the final Stage must remain byte-for-byte
identical to the reference source and behavioral tests.

## Trademark Notice

MiniKafka is an independent educational project. It is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation. "Apache Kafka" is a trademark of its respective owner.
