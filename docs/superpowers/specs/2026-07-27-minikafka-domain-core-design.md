# MiniKafka Domain Core Design

Date: 2026-07-27

Status: Approved under final-acceptance authorization

## 1. Purpose

MiniKafka is a direct-first reference implementation of Kafka's distinctive
product semantics. It is not a Kafka wire-protocol clone and is not a
production broker.

The project must make this path executable and observable:

```text
record
→ partition selection
→ per-partition batch
→ leader append and offset assignment
→ follower fetch
→ ISR and high-watermark advancement
→ consumer visibility
→ group ownership and offset commit
```

The finished repository is the implementation artifact. Course chapters are
designed later in a separate repository.

## 2. Chosen approach

Three approaches were considered:

1. **Protocol compatibility first.** Implement Kafka request headers, version
   negotiation, Produce, Fetch, and metadata APIs. This was rejected because
   binary compatibility would dominate the project while leaving core
   semantics shallow.
2. **Single-node log first and last.** Build segments, indexes, recovery,
   retention, and replay without groups or replication. This was rejected
   because it would omit Kafka's ownership, visibility, and acknowledgement
   semantics.
3. **Domain closure in layers.** Build the durable partition log first, then
   producer and consumer behavior, group coordination, log lifecycle,
   Kafka-specific replication, idempotence, and transaction visibility. This
   is the selected approach.

The implementation is Python 3.12 with no runtime dependencies. Tests use
pytest and deterministic clocks/failure gates. The package is named
`minikafka`.

## 3. Goals

The project implements:

- binary-safe records and versioned record batches;
- topics containing independently ordered partitions;
- monotonically increasing per-partition offsets;
- append-only segment files, segment rollover, sparse offset indexes, CRC,
  startup recovery, and tail truncation;
- deterministic producer partitioning and per-partition batching;
- consumer position, committed offsets, rewind, replay, lag, and reset policy;
- consumer groups with assignment, generation IDs, heartbeat expiry,
  rebalance, and stale-member fencing;
- time/size retention and key compaction with tombstones and offset gaps;
- partition leaders, pull-based followers, LEO, ISR, high watermark,
  `acks=0/1/all`, minimum ISR, leader epoch, promotion, and divergent-tail
  truncation;
- idempotent producers using producer ID, producer epoch, and per-partition
  sequence numbers;
- simplified transactional records, commit/abort markers,
  `read_uncommitted`/`read_committed`, and atomic transaction completion of
  output records plus input offsets;
- a Direct API as the primary interface and a bounded newline-delimited JSON
  TCP adapter as an end-to-end demonstration;
- deterministic performance and failure experiments.

## 4. Non-goals

The project does not implement:

- the Kafka wire protocol or binary compatibility with Kafka clients;
- KRaft, ZooKeeper, a controller quorum, or automatic controller election;
- production metadata propagation or dynamic broker discovery;
- SASL, TLS, ACLs, quotas, rack-aware placement, ELR, tiered storage,
  MirrorMaker, Connect, Streams, or Share Groups;
- a production transaction coordinator, internal transaction topic, or full
  transaction recovery across arbitrary coordinator failures;
- unclean leader election;
- all Kafka compression codecs or Kafka's exact record binary format;
- production page-cache, JVM, or throughput parity;
- application architecture exercises or course chapters.

## 5. Architecture and ownership

```text
Direct Admin / Producer / Consumer ──────┐
                                         ├── BrokerCluster
newline JSON TCP adapter ────────────────┘
                                              │
          ┌───────────────────────────────────┼─────────────────────────┐
          ▼                                   ▼                         ▼
   MetadataStore                       GroupCoordinator          TransactionManager
          │
          ▼
   PartitionReplicaSet
   ├── leader Replica ── PartitionLog ── Segment + SparseIndex
   ├── follower Replica ─ PartitionLog
   ├── ISR / HW / leader epoch
   └── ProducerStateManager
```

### 5.1 Ownership rules

- `BrokerCluster` owns topic metadata, broker lifecycle, replica placement,
  leader changes, and access to coordinators.
- One `PartitionReplicaSet` serializes all state changes for one topic
  partition. It owns leader epoch, ISR membership, high watermark, and
  acknowledgement waiters.
- Each `Replica` owns exactly one `PartitionLog`.
- `PartitionLog` owns its ordered immutable segment view, one active segment,
  file descriptors, and recovery.
- `GroupCoordinator` is the sole owner of group generations, membership,
  assignments, heartbeats, and committed offsets.
- `Producer` owns its accumulator, partitioning state, producer identity,
  epoch, and next sequence per partition.
- `TransactionManager` owns transaction state and fences producer epochs.
- Adapters translate requests and replies only. They do not own Kafka
  semantics or mutate logs directly.

All shared-state operations are guarded by component-local async locks.
No global lock is held across disk I/O or user callbacks.

## 6. Core model and offset conventions

### 6.1 Records

```python
Record(
    key: bytes | None,
    value: bytes | None,
    timestamp_ms: int,
    headers: tuple[Header, ...] = (),
)
```

Keys, values, and header values are binary-safe. A `None` value with a non-null
key is a tombstone. Header order is preserved.

### 6.2 Record batches

A batch contains one or more records and carries:

- format version;
- base offset and last offset delta;
- base/max timestamps;
- leader epoch;
- CRC32 over the stable payload;
- flags for compression, transaction, and control records;
- producer ID, producer epoch, and base sequence;
- optional transaction ID.

The custom format is length-delimited and versioned. Gzip is the only optional
compression codec. Broker append assigns offsets; the producer never chooses
them.

### 6.3 Offset boundaries

All public committed positions and log bounds use a next-offset convention:

- `log_start_offset`: first retained logical offset;
- `LEO`: next offset that would be assigned locally;
- `HW`: exclusive upper visibility bound;
- consumer `position`: next offset to fetch;
- committed group offset: next offset to resume.

A normal consumer may read only records satisfying:

```text
log_start_offset <= record.offset < visible_end
```

where `visible_end` is `HW` for replicated reads and `LEO` for an explicitly
uncommitted diagnostic read.

Compaction may create gaps. Fetch returns the first existing record at or
after the requested offset and advances position to one after the last
returned record.

## 7. Durable partition log

### 7.1 Files

```text
data/
└── broker-1/
    └── orders-0/
        ├── 00000000000000000000.log
        ├── 00000000000000000000.index
        ├── 00000000000000001024.log
        └── 00000000000000001024.index
```

Segment filenames use their base offset. The active segment accepts serial
appends. Rollover occurs before an append that would exceed
`segment_max_bytes`, except one oversized but otherwise valid batch may occupy
an empty segment.

### 7.2 Sparse index

The index stores fixed-width:

```text
relative offset → byte position
```

An entry is added when bytes written since the prior index entry reach
`index_interval_bytes`. Lookup binary-searches the segment and its sparse
index, then scans forward while verifying frames.

### 7.3 Recovery

Startup:

1. discovers and validates ordered segment pairs;
2. rebuilds a missing or invalid sparse index from the log;
3. scans the active segment frame by frame;
4. verifies length bounds, version, offset continuity, and CRC;
5. truncates an incomplete or corrupt tail to the last valid frame;
6. reconstructs LEO, producer state, transaction visibility, and timestamps.

Corruption in a closed segment is a startup error. Silent truncation is
limited to the active tail, where crash interruption is expected.

### 7.4 Flush contract

`flush()` fsyncs the active log and index. Configurable append durability is:

- `always`: flush before leader-local acknowledgement;
- `interval`: an injected scheduler flushes dirty logs;
- `manual`: tests or shutdown call `flush()`.

Replication acknowledgement and OS durability are separate dimensions and
are reported separately.

## 8. Producer

Partition selection precedence:

1. explicit partition;
2. stable CRC32 of a non-null key modulo partition count;
3. sticky partition for keyless records until its batch flushes, then rotate.

Each partition has an independent accumulator. A batch becomes sendable when
its encoded size reaches `batch_size`, its first record reaches `linger_ms`,
or `flush()` is called. One oversized valid record forms its own batch.

The producer preserves enqueue order inside each partition. It makes no
cross-partition order claim. Failed batches retain a deterministic retry order.
Without idempotence, a lost reply followed by retry may duplicate a batch.

`send()` returns a future for `RecordMetadata`. `acks=0` completes after local
adapter submission with unknown offsets represented as `None`; `acks=1`
completes after leader append; `acks=all` completes only after the batch end
is reached by every current ISR member and the ISR size meets minimum ISR.

## 9. Consumer and offsets

A consumer tracks a per-assigned-partition position independently from the
group's committed offset.

It supports:

- assignment or group subscription;
- `earliest` and `latest` initialization;
- fetch limits by records and encoded bytes;
- manual commits;
- simplified periodic auto-commit;
- seek/rewind;
- current position, committed position, and lag;
- explicit `earliest`, `latest`, or error behavior when an offset is out of
  the retained range.

Delivery experiments are first-class tests:

- commit then process demonstrates at-most-once loss;
- process then commit demonstrates at-least-once duplication;
- transactional output plus input offset demonstrates exactly-once-like
  read-process-write visibility within MiniKafka.

## 10. Consumer groups

The coordinator maintains:

```text
Group
├── generation
├── state: empty | preparing_rebalance | stable
├── members and session deadlines
├── subscriptions
├── assignment
└── committed next offsets
```

Joining, leaving, subscription changes, topic partition-count changes, and
heartbeat expiry trigger rebalance. Each completed rebalance increments the
generation.

The required assignor is deterministic round-robin over sorted topic
partitions and sorted member IDs. No partition may have two owners in one
stable generation.

Heartbeat time is injected. Expiry removes members and rebalances. Offset
commit requires:

- the group is stable;
- member ID and generation match;
- the member owns every committed partition.

Otherwise it fails with a fenced/stale-generation error. This prevents a
timed-out consumer from overwriting progress after ownership moved.

Coordinator offsets persist through an atomic temporary-file, fsync, rename,
and parent-directory fsync. Membership and heartbeat state are ephemeral.

## 11. Retention and compaction

Retention never deletes the active segment. Closed segments are deleted
oldest first when either:

- the segment's maximum timestamp is older than the time boundary; or
- total partition size exceeds the size budget.

Deletion updates the immutable segment view atomically and advances
`log_start_offset`. Consumers behind that boundary receive
`OffsetOutOfRange`.

Compaction:

1. chooses closed segments only;
2. determines the latest retained offset for each non-null key across the
   cleaning horizon;
3. preserves unkeyed records and the latest keyed record;
4. retains tombstones until `delete_retention_ms`, then may remove them;
5. preserves original offsets and therefore offset gaps;
6. writes new segment/index files in a temporary directory;
7. fsyncs and atomically swaps the cleaned segment set.

The active segment is never cleaned. Fetch, replay, producer state recovery,
and transaction markers must tolerate gaps.

## 12. Kafka-specific replication

### 12.1 Replica state

Each partition replica set contains one leader and zero or more followers.
Followers pull complete batches from the leader starting at their LEO.

```python
ReplicaState(
    broker_id: int,
    leo: int,
    last_fetch_ms: int,
    in_sync: bool,
)
```

ISR contains the leader plus followers whose lag by time and offset remains
within configured thresholds. ISR membership changes are explicit observable
events.

### 12.2 High watermark

`HW` is the minimum LEO across current ISR, expressed as an exclusive bound.
It is monotonically non-decreasing within a leader epoch. Consumers using the
normal path cannot read offsets at or above HW.

For a one-replica partition with minimum ISR one, leader append immediately
advances HW to leader LEO.

### 12.3 Acknowledgements

- `acks=0`: producer does not observe broker success or failure.
- `acks=1`: leader-local append is enough; failure before replication may lose
  an acknowledged record.
- `acks=all`: pre-append ISR size must meet minimum ISR, and completion waits
  for all replicas that were in the acknowledgement set to reach the batch
  end. Falling below minimum ISR before completion fails the request.

Consumer visibility follows HW, not the producer acknowledgement mode.

### 12.4 Leader change and truncation

Only an in-sync replica may be manually promoted. Promotion increments leader
epoch and fences requests carrying an older epoch.

The selected new leader defines the safe end. Other replicas truncate batches
beyond that boundary before fetching from the new leader. A returning old
leader cannot reintroduce an uncommitted divergent tail.

Automatic failure detection and leader election are non-goals. Tests invoke
failure and promotion explicitly so that Kafka's data-plane semantics remain
visible without rebuilding a generic consensus system.

## 13. Idempotent producer

The cluster allocates producer IDs. Initializing the same logical producer
name increments its producer epoch and fences the previous instance.

For each `(producer_id, topic, partition)`, the leader records the latest
accepted producer epoch, sequence range, and resulting offset range.

- exact retry of the latest accepted batch returns the original metadata
  without appending;
- first sequence equal to last sequence plus one is accepted;
- a lower non-matching sequence is a duplicate/invalid retry;
- a higher sequence is `OutOfOrderSequence`;
- an older producer epoch is fenced.

Producer state is rebuilt from retained batch headers on startup and
compaction preserves sufficient batch sequence boundaries for that rebuild.
Idempotence requires `acks=all`.

## 14. Transaction visibility

Transactions are intentionally bounded:

```text
begin(transactional_id)
→ produce to one or more partitions
→ stage input group offsets
→ commit or abort
```

Transactional data batches append normally but remain unstable. Commit or
abort appends control markers to every touched partition.

- `read_uncommitted` returns transactional data as soon as it is below HW and
  hides control records.
- `read_committed` returns only committed transaction data below the last
  stable offset and excludes aborted data and control records.
- staged group offsets become visible only after all commit markers append.
- abort markers discard staged offsets.

The transaction manager persists a compact journal sufficient for clean
restart and complete-marker recovery. Arbitrary coordinator failover,
distributed two-phase recovery, and production Kafka fencing edge cases remain
non-goals and are identified as such in public documentation.

## 15. Adapters

The Direct API is normative:

```python
cluster = await BrokerCluster.open(config)
await cluster.create_topic("orders", partitions=3, replication_factor=2)

producer = cluster.producer(acks="all", idempotent=True)
metadata = await producer.send("orders", key=b"user-42", value=b"...")

consumer = cluster.consumer(group_id="workers")
await consumer.subscribe(("orders",))
records = await consumer.poll(max_records=100)
await consumer.commit()
```

The optional TCP adapter accepts bounded newline-delimited JSON commands for
admin, produce, fetch, and group operations. Binary fields use base64.
Connection framing, throughput, Kafka client compatibility, and protocol
version negotiation are not targets.

Both adapters call the same cluster services and return the same domain
errors. Adapter success is never presented as Kafka wire compatibility.

## 16. Error model

Public domain errors are typed and stable:

- `UnknownTopic`, `UnknownPartition`;
- `InvalidRecord`, `RecordTooLarge`, `CorruptBatch`;
- `OffsetOutOfRange`;
- `NotLeader`, `FencedLeaderEpoch`;
- `NotEnoughReplicas`, `NotEnoughReplicasAfterAppend`;
- `RebalanceInProgress`, `IllegalGeneration`, `NotPartitionOwner`;
- `ProducerFenced`, `DuplicateSequence`, `OutOfOrderSequence`;
- `InvalidTransactionState`, `TransactionAborted`;
- `StorageError`, `AmbiguousProduceOutcome`.

Validation occurs before mutation where possible. Failures after a durable
append but before a reply are reported as ambiguous unless idempotent retry can
resolve them. Background task failures become observable terminal component
states; they are not swallowed.

## 17. Lifecycle and boundedness

All runtimes provide async context managers and idempotent close:

```text
accepting
→ quiescing
→ flush accumulators
→ resolve/fail waiters
→ persist offsets and transaction journal
→ flush logs
→ close files and tasks
→ closed
```

Configured bounds include:

- maximum record and batch bytes;
- accumulator bytes per producer;
- queued TCP frames and output bytes;
- fetch bytes and records;
- acknowledgement waiters;
- transaction records/bytes;
- segment and index sizes.

Simulated crash skips graceful flushing and closes descriptors, enabling
deterministic recovery tests.

## 18. Testing strategy

Every implementation slice follows red-green-refactor. Tests use temporary
directories, injected clocks, deterministic member and producer IDs, explicit
replication pumps, and failure gates.

Test layers:

1. **Codec/unit:** record batches, CRC, sparse index, partitioners, assignors.
2. **Log contract:** append/fetch, rollover, recovery, truncation, gaps.
3. **Producer/consumer:** batching, linger, ordering, position, commit, reset.
4. **Group mechanisms:** join/leave, expiry, rebalance, generation fencing.
5. **Lifecycle:** retention, compaction, tombstones, restart.
6. **Replication:** follower fetch, ISR changes, HW visibility, ack modes,
   leader promotion, truncation.
7. **Reliability:** lost replies, duplicate retries, producer fencing,
   transaction commit/abort visibility, crash recovery.
8. **Adapter parity:** Direct and JSON/TCP operations reach identical domain
   behavior.
9. **Acceptance:** one scenario crosses keyed production, batching,
   replication, committed consumption, group rebalance, restart, replay,
   compaction, and clean shutdown.

Tests assert semantics and invariants rather than sleeps. Performance labs
report measurements but do not impose unstable throughput thresholds.

## 19. Implementation phases

1. Project contract, errors, records, batches, and deterministic clock.
2. Segment, sparse index, partition log, recovery, and flush.
3. Topic metadata, Direct admin API, producer partitioning and batching.
4. Consumer position, commits, seek, lag, and offset persistence.
5. Consumer group membership, assignment, heartbeat, rebalance, and fencing.
6. Retention, compaction, tombstones, and atomic segment swap.
7. Replica sets, follower fetch, ISR, HW, ack modes, promotion, and truncation.
8. Producer identity, epochs, sequences, retry deduplication, and recovery.
9. Transactional batches, markers, isolation levels, and staged offsets.
10. JSON/TCP adapter, experiments, acceptance tests, behavior matrix, and
    documentation.

Each phase must leave the repository testable and internally consistent.

## 20. Acceptance criteria

The project is complete when fresh tests prove:

- offset assignment and replay survive segment rollover and restart;
- incomplete or corrupt active tails truncate at the last valid batch;
- the same key remains in one partition and order is partition-local;
- consumer position and committed offsets diverge and recover as documented;
- a stale group generation cannot commit offsets after rebalance;
- retention advances the start offset and compaction preserves offset gaps;
- records above HW are invisible;
- `acks=1` can lose an acknowledged tail after manual failover;
- `acks=all` waits for ISR replication and rejects insufficient ISR;
- returning replicas truncate divergent uncommitted tails;
- idempotent retry does not append twice and old producer epochs are fenced;
- aborted transaction records are hidden from `read_committed`;
- committed output records and staged input offsets become visible together;
- Direct and TCP adapters share semantics;
- crash, restart, and graceful shutdown leave no owned tasks or descriptors;
- the repository contains no course directory and makes no Kafka protocol
  compatibility claim.

## 21. Source alignment

The design intentionally follows Apache Kafka's documented concepts while
using a custom format and deterministic in-process cluster:

- log segments are serially appended, rolled by size, located by logical
  offset, deleted at segment granularity, and recovered by length/CRC scanning;
- records are written in batches whose headers carry producer and transaction
  metadata;
- `acks=1` may lose a leader-only append, while `acks=all` waits on in-sync
  replicas;
- minimum ISR constrains successful `acks=all` writes and consumer visibility.

The README will link the exact official documentation used for these claims
and separately enumerate every simplification.
