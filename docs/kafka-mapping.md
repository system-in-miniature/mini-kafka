> **Language**: English | [简体中文](zh/kafka-mapping.md)

# MiniKafka → Apache Kafka mapping

MiniKafka is a teaching model of Kafka's data plane, not a compatible broker.
This guide separates mechanisms that preserve Kafka's essential contract from
ones simplified for deterministic reading and testing.

The grades mean:

- **Equivalent** — the observable invariant matches Kafka within this
  project's stated boundary.
- **Intentional simplification** — the same idea is present, but protocol,
  persistence, scale, or background orchestration is reduced.
- **Semantically opposite** — the implementation takes a path Kafka
  deliberately does not; do not transfer that behavior to production Kafka.

## Log and storage

| Grade | MiniKafka module or mechanism | Kafka concept / configuration / KIP | Deliberate boundary |
| --- | --- | --- | --- |
| Equivalent | `core/record.py`: binary key/value, headers, timestamps and tombstones | Kafka record model; `message.timestamp.type` | Uses Python value objects and a custom binary encoding. |
| Intentional simplification | `core/batch_codec.py`: length-delimited batches with CRC32 | Kafka record batch v2; KIP-98 producer fields | `MKB1` is not Kafka's wire or on-disk format. |
| Equivalent | `log/partition_log.py`: append-only offsets and next-offset LEO | Partition log and log end offset | Ordering is partition-local and broker-assigned. |
| Equivalent | `log/segment.py`: size-based rolling | `log.segment.bytes` / topic `segment.bytes` | Only size rolling is implemented; no time-based roll. |
| Intentional simplification | `log/index.py`: sparse offset → byte-position index | `log.index.interval.bytes` | One compact index type; no time index or transaction index. |
| Intentional simplification | No time index or `offsetsForTimes` API | KIP-32 timestamps; ListOffsets / `offsetsForTimes` | Timestamp lookup is absent. |
| Equivalent | Recovery validates frame bounds and CRC, repairs only an active bad tail | Kafka log recovery and `CorruptRecordException` boundary | Closed-segment corruption is rejected rather than repaired. |
| Equivalent | `log/retention.py`: continuous-prefix deletion of closed segments | `cleanup.policy=delete`, `retention.ms`, `retention.bytes` | Current code stops at the first non-expired segment, so it cannot create a middle hole. |
| Equivalent | Compaction keeps the latest keyed record, tombstones for a grace period, and offset gaps | `cleanup.policy=compact`, `delete.retention.ms` | The logical record-selection contract matches; scheduling does not. |
| Intentional simplification | `LogCompactor` + `install_compacted_closed`: sibling-directory build and two-step rename | Kafka log cleaner, `log.cleaner.*` | Each rename is atomic and exceptions roll back, but the pair is not one atomic exchange; a process crash between renames can leave only the backup directory. |
| Intentional simplification | Batches are always uncompressed | Producer `compression.type`; topic `compression.type` | No gzip, snappy, lz4, or zstd flag/codec exists. |
| Intentional simplification | `PartitionLog.open` scans every batch to recover max leader epoch | `leader-epoch-checkpoint`; KIP-101 | Startup cost is O(all batches), despite sparse offset indexes. |
| Intentional simplification | `PartitionLog.truncate_to` reads retained batches, removes all segment files, and rebuilds one segment | Kafka tail truncation during replica reconciliation | O(all log) work and original segment boundaries are lost. |
| Equivalent | Metadata and committed offsets use tmp + fsync + rename + parent fsync | Kafka's durable checkpoint-file pattern | File schemas and ownership are MiniKafka-specific. |
| Intentional simplification | Producer identities use file fsync + atomic rename | Kafka producer-ID allocation and broker metadata durability | The local JSON store does not fsync its parent directory after rename. |

## Producer and acknowledgement path

| Grade | MiniKafka module or mechanism | Kafka concept / configuration / KIP | Deliberate boundary |
| --- | --- | --- | --- |
| Equivalent | Keyed CRC32 partitioning and explicit partition selection | Producer partitioner and `partitioner.class` | Hash function is project-specific, not Kafka's default murmur2 compatibility contract. |
| Intentional simplification | Sticky keyless partition plus per-partition accumulator | KIP-480 sticky partitioner; `batch.size`, `linger.ms`, `buffer.memory` | Flush scheduling is explicitly stepped for deterministic tests. |
| Equivalent | `acks=1` completes after the leader append | Producer `acks=1` | It can lose an acknowledged leader-only tail, as in Kafka. |
| Intentional simplification | `acks=0` returns unknown offsets after the Direct API has already performed the leader append | Producer `acks=0` | Kafka sends no broker response; MiniKafka can only model the caller's unknown outcome, not true fire-and-forget transport timing. |
| Intentional simplification | `acks=all` precheck, captured-ISR waiter, and fail-fast | Broker/topic `min.insync.replicas`; producer `acks=all` | MiniKafka waits for every replica captured at append time; Kafka completion follows its replication/HW machinery as ISR changes. |
| Intentional simplification | PID, producer epoch and sequence validation | KIP-98 idempotent producer; `enable.idempotence` | Identity allocation is a local JSON file, not broker coordination. |
| Intentional simplification | Duplicate window stores exactly the latest batch | Kafka retains five recent batches; `max.in.flight.requests.per.connection <= 5` | A retry of batch N arriving after N+1 was accepted becomes `OutOfOrderSequence`, not a duplicate hit. |
| Equivalent | A newer producer epoch fences an older instance | KIP-98 producer epoch fencing | Applies to idempotent producers, not MiniKafka transactions. |
| Intentional simplification | `Producer.send` is synchronous and returns an `asyncio.Future` | KafkaProducer asynchronous `send` | It must be called inside a running event loop. |

## Consumer and group coordination

| Grade | MiniKafka module or mechanism | Kafka concept / configuration / KIP | Deliberate boundary |
| --- | --- | --- | --- |
| Equivalent | Consumer position is separate from committed next offset; seek/replay/lag are partition-local | `enable.auto.commit`, `auto.offset.reset`, committed offsets | Fetch size controls and errors use the Direct API rather than Kafka requests. |
| Intentional simplification | `OffsetStore` persists commits in one atomic JSON file | Compacted internal topic `__consumer_offsets` | Offset commits do not exercise this project's own log/compaction stack. |
| Equivalent | Round-robin gives one stable-generation owner per partition | `RoundRobinAssignor`; `partition.assignment.strategy` | Only one deterministic assignor is supplied. |
| Intentional simplification | Join immediately performs server-side assignment and becomes stable | Classic JoinGroup/SyncGroup group-coordinator protocol | No group leader, sync phase, rebalance timeout, or revocation barrier. |
| Intentional simplification | Old local assignment remains until `refresh_assignment()` | Eager rebalance and KIP-429 cooperative rebalance | Before refresh, two local consumers may temporarily believe they own the same partition; stale commits are still fenced. |
| Equivalent | Generation checked on heartbeat and commit; ownership checked on commit | Classic group generation fencing; `session.timeout.ms` | A stale generation cannot overwrite the new owner's committed offset. |
| Intentional simplification | Heartbeat expiry is driven by an injected clock and explicit call | `session.timeout.ms`, `heartbeat.interval.ms`; KIP-62 | No background heartbeat thread or coordinator network timeout. |
| Intentional simplification | Unknown group currently raises `UnknownMember` | Kafka `GROUP_ID_NOT_FOUND` / `UNKNOWN_MEMBER_ID` distinctions | Error taxonomy is collapsed and the name can be misleading. |

## Replication and failover

| Grade | MiniKafka module or mechanism | Kafka concept / configuration / KIP | Deliberate boundary |
| --- | --- | --- | --- |
| Equivalent | Followers pull complete batches from leader LEO | Kafka follower Fetch replication | A method call replaces broker-to-broker networking. |
| Intentional simplification | ISR requires time lag, offset lag, and coverage of current HW | `replica.lag.time.max.ms`; historical `replica.lag.max.messages` | Modern Kafka uses the time rule; the offset cap and HW gate make state transitions deterministic and safe in the mini model. |
| Equivalent | Within one running replica-set instance, HW is the minimum LEO across current ISR and bounds normal reads | Kafka partition high watermark | HW is exclusive and advances monotonically while that in-memory leader instance runs. |
| Intentional simplification | HW is not checkpointed; startup initializes it from all replica LEOs | Kafka replication-offset checkpoint files | A lagging replica can temporarily move restarted visibility below a previously consumed offset. |
| Equivalent | Leader epoch is stored in metadata and batches and fences stale requests | KIP-101 leader epochs | Epoch is present, but divergence detection does not use its history. |
| Semantically opposite | Promotion/rejoin/follower catch-up truncate at HW rather than epoch divergence | KIP-101 and KIP-279 leader-epoch reconciliation | This is pre-KIP-101-style reasoning and may discard a tail that epoch comparison would retain. |
| Semantically opposite | `promote()` may truncate the newly selected leader itself to HW | Kafka leader election: the new leader's LEO defines the endpoint and followers align to it | This creates a MiniKafka-only loss window for data present on all ISR replicas before HW advancement. |
| Intentional simplification | Promotion and failure detection are explicit calls | Controller election, KRaft (KIP-500) | No automatic election, controller quorum, or unclean election. |
| Intentional simplification | Single process hosts all broker replicas | Kafka brokers are separate failure domains | Disk layouts are separate, but process, scheduler, and host failures are shared. |

## Transactions and visibility

| Grade | MiniKafka module or mechanism | Kafka concept / configuration / KIP | Deliberate boundary |
| --- | --- | --- | --- |
| Equivalent | Transactional records plus commit/abort control markers and LSO-filtered `read_committed` | KIP-98 transaction markers; consumer `isolation.level` | Coordinator and journal are local and bounded. |
| Equivalent | Transaction data and markers currently append with `acks=all` | Kafka transactional production requires durable replication | This was corrected in the pre-existing worktree changes reviewed alongside this document. |
| Intentional simplification | Transaction writes use no idempotent PID/epoch/sequence state | Kafka transactions build on idempotence and `transactional.id` zombie fencing (KIP-98) | `transactional_id` and the transaction journal are separate from `ProducerStateManager`; `acks=all` alone is not Kafka's transaction protocol. |
| Equivalent | Staged input offsets publish only after commit markers; abort discards them | `sendOffsetsToTransaction` read-process-write pattern | Atomicity is within one in-process coordinator, not arbitrary coordinator failover. |
| Intentional simplification | Journal recovery completes PREPARE states locally | Kafka transaction state log and transaction coordinator | No replicated `__transaction_state` internal topic or coordinator migration. |

## API, runtime, and excluded systems

| Grade | MiniKafka module or mechanism | Kafka concept / configuration / KIP | Deliberate boundary |
| --- | --- | --- | --- |
| Intentional simplification | `BrokerCluster` Direct API is normative | Kafka request/response protocol | It teaches domain state transitions without protocol version machinery. |
| Intentional simplification | Newline-delimited JSON/TCP adapter | Kafka binary wire protocol | It proves adapter separation only; Kafka clients cannot connect. |
| Intentional simplification | Replication, linger flushes, retention and expiry are manually stepped | Broker and client background threads | Determinism replaces wall-clock concurrency in tests and labs. |
| Intentional simplification | Custom lifecycle and flush modes | Kafka log flush / shutdown configuration | No JVM page-cache or throughput equivalence is claimed. |
| Intentional simplification | Elections, KRaft/ZooKeeper, security, quotas, rack awareness and tiered storage are excluded | KIP-500, KIP-405, broker security and quota configs | These are explicit non-goals, not partially implemented features. |

## How to read the mapping

Start with rows marked **Equivalent** to learn the invariant: partition-local
order, next-offset boundaries, HW visibility, stable ownership, and fencing.
Then read each adjacent simplification before transferring the idea to a real
Kafka deployment. Rows marked **Semantically opposite** are warnings: the
code is useful for exposing a failure boundary, but its failover truncation
must not be treated as Kafka behavior.
