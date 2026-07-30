# MiniKafka Tutorial

This ten-chapter course treats MiniKafka as an executable teaching kernel for
Kafka's distinctive data-plane mechanisms. Read it in order: storage establishes
the offset model, producers and replication build on that model, and consumer
groups and transactions add coordination and visibility boundaries.

Every chapter includes source-grounded explanations, an explicit comparison
with Apache Kafka, measured experiments, and exercises with acceptance criteria.
MiniKafka is not Kafka wire-protocol compatible; use the reference material to
keep modeled semantics and production parity separate.

## Chapters

1. [Meet MiniKafka](01-getting-started.md) — teaching-kernel scope,
   environment, and the first direct-API produce→fetch.
2. [The Log: Foundation of Everything](02-the-log.md) — segment rolling,
   sparse offset indexes, CRC frames, and active-tail recovery.
3. [Retention and Compaction](03-retention-compaction.md) — prefix retention,
   keyed compaction, tombstones, and directory replacement.
4. [The Producer](04-producer.md) — keyed/sticky partitioning, batching,
   linger, and PID/epoch/sequence idempotence.
5. [Replication I: Followers and Watermarks](05-replication-basics.md) —
   follower pulls, LEO, HW, visibility, and acknowledgement modes.
6. [Replication II: ISR and Fencing](06-isr-and-fencing.md) — ISR shrink and
   recovery, minimum ISR, leader epochs, and promotion.
7. [Consumer Groups](07-consumer-groups.md) — generation fencing, heartbeats,
   assignment, offset commits, and rebalance boundaries.
8. [Transactions](08-transactions.md) — journal recovery, markers, LSO,
   `read_committed`, and atomic offset publication.
9. [Delivery Semantics Lab](09-delivery-semantics.md) — acknowledged-write
   loss, idempotent retry, and the ingredients of exactly-once processing.
10. [Protocol and Beyond](10-protocol-and-beyond.md) — the thin TCP adapter,
    wire-protocol differences, and a path into real Kafka.

## Reference material

- [Quick start and project overview](../index.md)
- [Architecture](../architecture.md)
- [MiniKafka → Kafka mapping](../kafka-mapping.md)
- [Hands-on labs](../labs-guide.md)
- [Behavior matrix and executable evidence](../behavior-matrix.md)

For the complete Chinese edition, see the
[中文教程目录](../zh/tutorial/index.md).
