# MiniKafka Domain Core Design and Implementation History

**Historical objective:** Build a direct-first MiniKafka whose durable partition log, consumer ownership, ISR/high-watermark visibility, idempotent retry, and transaction visibility can be executed and failure-tested.

**Architecture:** A Python `BrokerCluster` owns topic metadata and coordinators; each partition replica set serializes Kafka-specific replication state while each replica owns a disk-backed `PartitionLog`. Direct clients are normative and a thin JSON/TCP adapter delegates to the same services.

**Tech Stack:** Python 3.12, standard library (`asyncio`, `dataclasses`, `pathlib`, `struct`, `zlib`, `gzip`, `json`), hatchling, uv, pytest 9, pytest-asyncio, Ruff.

---

## File map

```text
pyproject.toml                         build, dependencies, test/lint config
README.md                              project contract and examples
docs/behavior-matrix.md                feature-to-test evidence
src/minikafka/
├── __init__.py                        stable public API
├── clock.py                           injected real/manual clocks
├── config.py                          validated immutable configuration
├── errors.py                          typed domain failures
├── lifecycle.py                       runtime states and task ownership
├── core/
│   ├── record.py                      Record/Header/StoredRecord
│   ├── batch.py                       RecordBatch model
│   ├── batch_codec.py                 custom binary framing and CRC
│   ├── metadata.py                    topics, partitions, replica assignments
│   └── cluster.py                     BrokerCluster façade and lifecycle
├── log/
│   ├── index.py                       fixed-width sparse offset index
│   ├── segment.py                     one log/index pair
│   ├── partition_log.py               segment set, append, fetch, truncate
│   ├── recovery.py                    startup discovery and tail repair
│   ├── retention.py                   time/size segment deletion
│   └── compaction.py                  keyed cleaning and atomic swap
├── producer/
│   ├── partitioner.py                 explicit/keyed/sticky selection
│   ├── accumulator.py                 bounded per-partition batches
│   ├── state.py                       producer ID/epoch/sequence state
│   └── producer.py                    send/flush/retry API
├── consumer/
│   ├── offsets.py                     atomic committed-offset persistence
│   ├── consumer.py                    position, poll, seek, commit, lag
│   ├── assignor.py                    deterministic round-robin assignment
│   └── group.py                       group state, heartbeat, rebalance, fence
├── replication/
│   ├── model.py                       AckMode, ReplicaState, ProduceResult
│   ├── replica.py                     replica-local log wrapper
│   └── replica_set.py                 follower fetch, ISR, HW, epochs
├── transaction/
│   ├── model.py                       transaction state and control marker
│   ├── journal.py                     compact persistent transaction journal
│   └── manager.py                     begin/commit/abort and isolation
└── adapters/
    ├── direct.py                       direct admin/client façades
    └── json_tcp.py                     bounded newline JSON transport
tests/
├── unit/                               codec/index/partitioner/assignor tests
├── log/                                append/recovery/retention/compaction
├── producer/                           batching/order/idempotence
├── consumer/                           positions/groups/fencing
├── replication/                        ISR/HW/acks/promotion/truncation
├── transaction/                        visibility/offset atomicity/recovery
├── adapters/                           Direct/TCP parity
├── reliability/                        deterministic crash/failure scenarios
└── test_final_acceptance.py            end-to-end project contract
```

### Milestone 1: Project contract and primitives

**Recorded file scope:**
- Added: `pyproject.toml`
- Added: `.gitignore`
- Added: `src/minikafka/__init__.py`
- Added: `src/minikafka/clock.py`
- Added: `src/minikafka/config.py`
- Added: `src/minikafka/errors.py`
- Added: `tests/unit/test_primitives.py`
- Added: `tests/test_project_contract.py`

**Recorded activity 1 — Test intent: failing primitive and repository-contract tests**

```python
from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.errors import OffsetOutOfRange


def test_manual_clock_is_deterministic() -> None:
    clock = ManualClock(100)
    clock.advance_ms(25)
    assert clock.now_ms() == 125


def test_config_rejects_invalid_segment_size(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="segment_max_bytes"):
        MiniKafkaConfig(data_dir=tmp_path, segment_max_bytes=0)


def test_error_carries_stable_code() -> None:
    assert OffsetOutOfRange(3, 5, 9).code == "OFFSET_OUT_OF_RANGE"


def test_course_is_not_embedded() -> None:
    assert not Path("course").exists()
```

**Recorded activity 2 — Verification intent: tests and verify collection fails**

Historical verification covered targeted or full test coverage, including `tests/unit/test_primitives.py`, `tests/test_project_contract.py`.

Historical expected evidence: FAIL because `minikafka` and project metadata do not exist.

**Recorded activity 3 — Design outcome: the build contract and minimal primitives**

```python
@dataclass
class ManualClock:
    _now_ms: int = 0

    def now_ms(self) -> int:
        return self._now_ms

    def advance_ms(self, amount: int) -> None:
        if amount < 0:
            raise ValueError("clock cannot move backwards")
        self._now_ms += amount


class OffsetOutOfRange(MiniKafkaError):
    code = "OFFSET_OUT_OF_RANGE"

    def __init__(self, requested: int, start: int, end: int) -> None:
        super().__init__(
            f"offset {requested} outside retained range [{start}, {end}]"
        )
```

The recorded project configuration used hatchling for `src/minikafka`, Python `>=3.12`, pytest asyncio mode,
Ruff line length 88, and dev dependencies `pytest>=9,<10`,
`pytest-asyncio>=1.3,<2`, and `ruff>=0.12,<1`.

**Recorded activity 4 — Verification intent: primitive tests and lint**

Historical verification covered targeted or full test coverage, static analysis, including `tests/unit/test_primitives.py`, `tests/test_project_contract.py`.

Historical expected evidence: all tests pass and Ruff reports no errors.

### Milestone 2: Record batches and binary codec

**Recorded file scope:**
- Added: `src/minikafka/core/__init__.py`
- Added: `src/minikafka/core/record.py`
- Added: `src/minikafka/core/batch.py`
- Added: `src/minikafka/core/batch_codec.py`
- Added: `tests/unit/test_batch_codec.py`

**Recorded activity 1 — Test intent: failing round-trip, CRC, and binary-safety tests**

```python
def test_batch_round_trip_preserves_binary_records() -> None:
    batch = RecordBatch.unassigned(
        records=(
            Record(key=b"k\x00", value=b"v\xff", timestamp_ms=7),
            Record(key=b"k2", value=None, timestamp_ms=8),
        ),
        producer_id=4,
        producer_epoch=2,
        base_sequence=9,
    )
    stored = batch.assign(base_offset=12, leader_epoch=3)
    assert decode_batch(encode_batch(stored)) == stored


def test_crc_detects_payload_corruption() -> None:
    encoded = bytearray(encode_batch(sample_batch()))
    encoded[-1] ^= 0x01
    with pytest.raises(CorruptBatch):
        decode_batch(bytes(encoded))
```

**Recorded activity 2 — Verification intent: the codec tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/unit/test_batch_codec.py`.

Historical expected evidence: FAIL with missing record/batch modules.

**Recorded activity 3 — Design outcome: immutable records, assignment, and framing**

```python
@dataclass(frozen=True, slots=True)
class Record:
    key: bytes | None
    value: bytes | None
    timestamp_ms: int
    headers: tuple[Header, ...] = ()


@dataclass(frozen=True, slots=True)
class RecordBatch:
    records: tuple[Record, ...]
    base_offset: int | None
    leader_epoch: int
    producer_id: int
    producer_epoch: int
    base_sequence: int
    transactional_id: str | None = None
    control: ControlType | None = None

    @property
    def next_offset(self) -> int:
        if self.base_offset is None:
            raise ValueError("batch is not assigned")
        return self.base_offset + len(self.records)
```

The design used a fixed prefix `(magic, frame_length, crc)` followed by a deterministic
payload containing flags, offsets, timestamps, producer metadata, transaction
ID, and length-delimited records. Reject empty data batches, negative lengths,
unsupported versions, trailing bytes, and frames above `max_batch_bytes`.

**Recorded activity 4 — Verification intent: codec tests**

Historical verification covered targeted or full test coverage, including `tests/unit/test_batch_codec.py`.

Historical expected evidence: PASS.

### Milestone 3: Sparse offset index

**Recorded file scope:**
- Added: `src/minikafka/log/__init__.py`
- Added: `src/minikafka/log/index.py`
- Added: `tests/unit/test_offset_index.py`

**Recorded activity 1 — Test intent: failing floor-lookup and reload tests**

```python
def test_sparse_index_returns_floor_position(tmp_path: Path) -> None:
    index = OffsetIndex.create(tmp_path / "000.index", base_offset=100)
    index.append(offset=100, position=0)
    index.append(offset=110, position=512)
    index.append(offset=125, position=900)
    assert index.floor_position(117) == 512
    index.close()
    assert OffsetIndex.open(tmp_path / "000.index", 100).floor_position(125) == 900


def test_index_rejects_non_monotonic_entries(tmp_path: Path) -> None:
    index = OffsetIndex.create(tmp_path / "000.index", base_offset=0)
    index.append(offset=5, position=20)
    with pytest.raises(ValueError, match="monotonic"):
        index.append(offset=4, position=30)
```

**Recorded activity 2 — Verification intent: the index tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/unit/test_offset_index.py`.

Historical expected evidence: FAIL because `OffsetIndex` is undefined.

**Recorded activity 3 — Design outcome: fixed-width index entries**

```python
ENTRY = struct.Struct(">IQ")


def append(self, offset: int, position: int) -> None:
    relative = offset - self.base_offset
    if relative < 0 or relative > 0xFFFFFFFF:
        raise ValueError("relative offset out of range")
    if self._entries and (
        relative <= self._entries[-1][0] or position <= self._entries[-1][1]
    ):
        raise ValueError("index entries must be monotonic")
    self._file.write(ENTRY.pack(relative, position))
    self._entries.append((relative, position))
```

Load complete entries only; reject partial/corrupt index files so callers can
rebuild them from the log. Binary-search entries with `bisect_right`.

**Recorded activity 4 — Verification intent: index tests**

Historical verification covered targeted or full test coverage, including `tests/unit/test_offset_index.py`.

Historical expected evidence: PASS.

### Milestone 4: Segments and active-tail recovery

**Recorded file scope:**
- Added: `src/minikafka/log/segment.py`
- Added: `src/minikafka/log/recovery.py`
- Added: `tests/log/test_segment.py`
- Added: `tests/log/test_recovery.py`

**Recorded activity 1 — Test intent: failing append/scan/truncated-tail tests**

```python
def test_segment_appends_and_scans_from_offset(tmp_path: Path) -> None:
    segment = Segment.create(tmp_path, base_offset=0, index_interval_bytes=1)
    segment.append(assigned_batch(0, values=(b"a", b"b")))
    segment.append(assigned_batch(2, values=(b"c",)))
    assert [r.offset for r in segment.scan(1)] == [1, 2]


def test_active_tail_is_truncated_to_last_valid_batch(tmp_path: Path) -> None:
    segment = Segment.create(tmp_path, base_offset=0, index_interval_bytes=8)
    segment.append(assigned_batch(0, values=(b"safe",)))
    valid_size = segment.size_bytes
    segment.raw_append_for_test(b"\x00\x00\x00\x20partial")
    segment.close()
    recovered = recover_active_segment(tmp_path, base_offset=0, config=config())
    assert recovered.size_bytes == valid_size
    assert recovered.leo == 1
```

**Recorded activity 2 — Verification intent: tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/log/test_segment.py`, `tests/log/test_recovery.py`.

Historical expected evidence: FAIL because segment/recovery APIs do not exist.

**Recorded activity 3 — Design outcome: segment ownership and repair**

```python
def append(self, batch: RecordBatch) -> BatchLocation:
    if batch.base_offset != self.leo:
        raise ValueError(f"expected base offset {self.leo}")
    encoded = encode_batch(batch)
    position = self._log.tell()
    self._log.write(FRAME_LENGTH.pack(len(encoded)))
    self._log.write(encoded)
    if position - self._last_index_position >= self.index_interval_bytes:
        self.index.append(batch.base_offset, position)
        self._last_index_position = position
    self.leo = batch.next_offset
    return BatchLocation(position, len(encoded) + FRAME_LENGTH.size)
```

Recovery scans `[length][encoded batch]`, verifies bounds/CRC/offset
continuity, and truncates only the active file at the first invalid frame.
Rebuild the index from valid frame positions.

**Recorded activity 4 — Verification intent: segment and recovery tests**

Historical verification covered targeted or full test coverage, including `tests/log/test_segment.py`, `tests/log/test_recovery.py`.

Historical expected evidence: PASS.

### Milestone 5: Partition log and segment rollover

**Recorded file scope:**
- Added: `src/minikafka/log/partition_log.py`
- Added: `tests/log/test_partition_log.py`
- Added: `tests/reliability/test_log_restart.py`

**Recorded activity 1 — Test intent: failing rollover, offset lookup, and restart tests**

```python
def test_partition_log_rolls_and_fetches_across_segments(tmp_path: Path) -> None:
    log = PartitionLog.open(tmp_path, config(segment_max_bytes=120))
    for value in (b"a" * 40, b"b" * 40, b"c" * 40):
        log.append(RecordBatch.unassigned((record(value),)))
    assert len(log.segments) >= 2
    assert [r.value for r in log.fetch(1, max_records=10)] == [
        b"b" * 40,
        b"c" * 40,
    ]


def test_restart_preserves_leo_and_offsets(tmp_path: Path) -> None:
    log = PartitionLog.open(tmp_path, config(segment_max_bytes=120))
    log.append(RecordBatch.unassigned((record(b"x"), record(b"y"))))
    log.flush()
    log.close()
    reopened = PartitionLog.open(tmp_path, config(segment_max_bytes=120))
    assert reopened.leo == 2
    assert [r.offset for r in reopened.fetch(0, 10)] == [0, 1]
```

**Recorded activity 2 — Verification intent: partition-log tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/log/test_partition_log.py`, `tests/reliability/test_log_restart.py`.

Historical expected evidence: FAIL because `PartitionLog` is missing.

**Recorded activity 3 — Design outcome: immutable segment views and logical fetch**

```python
def append(self, batch: RecordBatch) -> AppendInfo:
    assigned = batch.assign(self.leo, self.leader_epoch)
    encoded_size = encoded_frame_size(assigned)
    if self.active.has_data and self.active.size_bytes + encoded_size > self.segment_max_bytes:
        self._roll(self.leo)
    location = self.active.append(assigned)
    self.leo = assigned.next_offset
    return AppendInfo(assigned.base_offset, assigned.next_offset, location)


def fetch(self, offset: int, max_records: int, end_offset: int | None = None):
    if offset < self.log_start_offset or offset > self.leo:
        raise OffsetOutOfRange(offset, self.log_start_offset, self.leo)
    limit = self.leo if end_offset is None else min(end_offset, self.leo)
    return tuple(self._iter_existing(offset, limit, max_records))
```

Closed-segment corruption raises `StorageError`; only active-tail repair is
automatic. `truncate_to(next_offset)` removes later segments and rewrites the
containing segment at a batch boundary.

**Recorded activity 4 — Verification intent: log tests and the whole current suite**

Historical verification covered targeted or full test coverage, including `tests/log`, `tests/reliability/test_log_restart.py`.

Historical expected evidence: PASS.

### Milestone 6: Topic metadata and Direct cluster

**Recorded file scope:**
- Added: `src/minikafka/core/metadata.py`
- Added: `src/minikafka/core/cluster.py`
- Added: `src/minikafka/adapters/__init__.py`
- Added: `src/minikafka/adapters/direct.py`
- Added: `tests/unit/test_metadata.py`
- Added: `tests/test_direct_cluster.py`

**Recorded activity 1 — Test intent: failing metadata and Direct API tests**

```python
@pytest.mark.asyncio
async def test_create_topic_builds_partition_logs(tmp_path: Path) -> None:
    async with BrokerCluster.open(config(tmp_path, broker_ids=(1, 2))) as cluster:
        topic = await cluster.create_topic("orders", 3, replication_factor=2)
        assert tuple(topic.partitions) == (0, 1, 2)
        assert topic.partitions[0].replicas == (1, 2)
        with pytest.raises(TopicAlreadyExists):
            await cluster.create_topic("orders", 1, 1)
```

**Recorded activity 2 — Verification intent: Direct-cluster tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/unit/test_metadata.py`, `tests/test_direct_cluster.py`.

Historical expected evidence: FAIL because cluster and metadata do not exist.

**Recorded activity 3 — Design outcome: validated metadata and cluster lifecycle**

```python
@dataclass(frozen=True, slots=True)
class PartitionMetadata:
    topic: str
    partition: int
    replicas: tuple[int, ...]
    leader_id: int
    leader_epoch: int = 0


async def create_topic(
    self, name: str, partitions: int, replication_factor: int
) -> TopicMetadata:
    validate_topic_name(name)
    if name in self._topics:
        raise TopicAlreadyExists(name)
    assignment = round_robin_replicas(self.broker_ids, partitions, replication_factor)
    topic = TopicMetadata.from_assignment(name, assignment)
    await self._open_replica_logs(topic)
    self._topics[name] = topic
    await self._metadata_store.save(self._topics)
    return topic
```

Persist metadata with temp-file, fsync, atomic rename, and parent fsync.
`DirectAdmin` delegates to the cluster without alternate semantics.

**Recorded activity 4 — Verification intent: Direct cluster tests**

Historical verification covered targeted or full test coverage, including `tests/unit/test_metadata.py`, `tests/test_direct_cluster.py`.

Historical expected evidence: PASS.

### Milestone 7: Producer partitioning and batching

**Recorded file scope:**
- Added: `src/minikafka/producer/__init__.py`
- Added: `src/minikafka/producer/partitioner.py`
- Added: `src/minikafka/producer/accumulator.py`
- Added: `src/minikafka/producer/producer.py`
- Added: `tests/unit/test_partitioner.py`
- Added: `tests/producer/test_batching.py`
- Added: `tests/producer/test_ordering.py`

**Recorded activity 1 — Test intent: failing keyed, sticky, batch-size, and linger tests**

```python
def test_keyed_partition_is_stable() -> None:
    partitioner = Partitioner()
    assert partitioner.choose(3, key=b"user-42") == partitioner.choose(
        3, key=b"user-42"
    )


@pytest.mark.asyncio
async def test_linger_flushes_one_partition_batch(cluster, manual_clock) -> None:
    producer = cluster.producer(batch_size=4096, linger_ms=10)
    first = producer.send("events", key=b"k", value=b"1")
    second = producer.send("events", key=b"k", value=b"2")
    manual_clock.advance_ms(10)
    await producer.run_due_flushes()
    assert (await first).base_offset == 0
    assert (await second).offset == 1
    assert cluster.debug_batches("events", (await first).partition) == 1
```

**Recorded activity 2 — Verification intent: producer tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/unit/test_partitioner.py`, `tests/producer`.

Historical expected evidence: FAIL because producer components are missing.

**Recorded activity 3 — Design outcome: stable selection and bounded accumulators**

```python
def choose(self, partition_count: int, key: bytes | None) -> int:
    if key is not None:
        return zlib.crc32(key) % partition_count
    return self._sticky_partition(partition_count)


def add(self, pending: PendingRecord) -> tuple[PendingBatch, ...]:
    queue = self._by_partition[pending.topic_partition]
    if self.total_bytes + pending.estimated_bytes > self.max_buffer_bytes:
        raise ProducerBufferFull()
    queue.append(pending)
    self.total_bytes += pending.estimated_bytes
    return self._drain_full_batches(pending.topic_partition)
```

`Producer.send()` returns an awaitable future, explicit partitions override the
partitioner, keyless selection rotates after a batch closes, and `flush()`
drains all partitions in deterministic topic/partition order.

**Recorded activity 4 — Verification intent: producer tests**

Historical verification covered targeted or full test coverage, including `tests/unit/test_partitioner.py`, `tests/producer`.

Historical expected evidence: PASS.

### Milestone 8: Consumer positions and durable committed offsets

**Recorded file scope:**
- Added: `src/minikafka/consumer/__init__.py`
- Added: `src/minikafka/consumer/offsets.py`
- Added: `src/minikafka/consumer/consumer.py`
- Added: `tests/consumer/test_positions.py`
- Added: `tests/consumer/test_delivery_semantics.py`
- Added: `tests/reliability/test_offset_restart.py`

**Recorded activity 1 — Test intent: failing position/commit/rewind/restart tests**

```python
@pytest.mark.asyncio
async def test_position_and_commit_are_independent(cluster) -> None:
    await produce_values(cluster, "events", [b"0", b"1", b"2"])
    consumer = cluster.consumer(group_id="g", auto_offset_reset="earliest")
    consumer.assign((TopicPartition("events", 0),))
    records = await consumer.poll(max_records=2)
    assert [r.offset for r in records] == [0, 1]
    assert consumer.position(TopicPartition("events", 0)) == 2
    assert await consumer.committed(TopicPartition("events", 0)) is None
    await consumer.commit()
    consumer.seek(TopicPartition("events", 0), 0)
    assert (await consumer.poll(1))[0].offset == 0
```

**Recorded activity 2 — Verification intent: consumer tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/consumer`, `tests/reliability/test_offset_restart.py`.

Historical expected evidence: FAIL because consumer components are missing.

**Recorded activity 3 — Design outcome: next-offset positions and atomic offset store**

```python
async def poll(self, max_records: int) -> tuple[StoredRecord, ...]:
    result: list[StoredRecord] = []
    for tp in self._assignment:
        position = self._positions.setdefault(tp, await self._initial_position(tp))
        fetched = await self._cluster.fetch(tp, position, max_records - len(result))
        if fetched:
            self._positions[tp] = fetched[-1].offset + 1
            result.extend(fetched)
        if len(result) == max_records:
            break
    return tuple(result)


async def commit(self) -> None:
    await self._offset_store.commit(self.group_id, dict(self._positions))
```

Offset persistence uses deterministic JSON keys and atomic replace. Implement
lag as `visible_end - position` and reset policies for retained-range errors.

**Recorded activity 4 — Verification intent: consumer and restart tests**

Historical verification covered targeted or full test coverage, including `tests/consumer`, `tests/reliability/test_offset_restart.py`.

Historical expected evidence: PASS.

### Milestone 9: Consumer groups, rebalance, and fencing

**Recorded file scope:**
- Added: `src/minikafka/consumer/assignor.py`
- Added: `src/minikafka/consumer/group.py`
- Added: `tests/unit/test_assignor.py`
- Added: `tests/consumer/test_group_rebalance.py`
- Added: `tests/consumer/test_generation_fencing.py`

**Recorded activity 1 — Test intent: failing assignment and stale-commit tests**

```python
def test_round_robin_assignor_has_single_owner() -> None:
    assignment = round_robin_assign(
        members={"a": {"orders"}, "b": {"orders"}},
        partitions={"orders": (0, 1, 2, 3)},
    )
    assert assignment["a"] == (
        TopicPartition("orders", 0),
        TopicPartition("orders", 2),
    )
    assert assignment["b"] == (
        TopicPartition("orders", 1),
        TopicPartition("orders", 3),
    )


@pytest.mark.asyncio
async def test_expired_member_cannot_commit_old_generation(group, clock) -> None:
    old = await group.join("a", {"orders"})
    clock.advance_ms(group.session_timeout_ms + 1)
    await group.expire_members()
    current = await group.join("b", {"orders"})
    with pytest.raises(IllegalGeneration):
        await group.commit("a", old.generation, {old.assignment[0]: 1})
    await group.commit("b", current.generation, {current.assignment[0]: 1})
```

**Recorded activity 2 — Verification intent: group tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/unit/test_assignor.py`, `tests/consumer/test_group_rebalance.py`, `tests/consumer/test_generation_fencing.py`.

Historical expected evidence: FAIL because assignor/coordinator are missing.

**Recorded activity 3 — Design outcome: group state and ownership checks**

```python
async def _rebalance(self) -> None:
    self.state = GroupState.PREPARING_REBALANCE
    self.generation += 1
    self.assignment = round_robin_assign(
        {m.member_id: m.subscriptions for m in self.members.values()},
        self.topic_partitions(),
    )
    self.state = GroupState.STABLE if self.members else GroupState.EMPTY


def _validate_commit(self, member_id: str, generation: int, offsets) -> None:
    if generation != self.generation:
        raise IllegalGeneration(generation, self.generation)
    owned = set(self.assignment.get(member_id, ()))
    if not set(offsets).issubset(owned):
        raise NotPartitionOwner(member_id)
```

Every membership/subscription change and expiry increments generation through
rebalance. Consumer group subscription refreshes its assignment after a
generation change.

**Recorded activity 4 — Verification intent: group tests**

Historical verification covered targeted or full test coverage, including `tests/unit/test_assignor.py`, `tests/consumer`.

Historical expected evidence: PASS.

### Milestone 10: Retention

**Recorded file scope:**
- Added: `src/minikafka/log/retention.py`
- Added: `tests/log/test_retention.py`

**Recorded activity 1 — Test intent: failing time/size and out-of-range tests**

```python
def test_retention_deletes_closed_segments_only(tmp_path: Path, clock) -> None:
    log = populated_rolled_log(tmp_path, clock)
    active_base = log.active.base_offset
    deleted = RetentionManager(clock).apply(
        log, retention_ms=100, retention_bytes=200
    )
    assert deleted
    assert log.active.base_offset == active_base
    assert log.log_start_offset > 0
    with pytest.raises(OffsetOutOfRange):
        log.fetch(0, 1)
```

**Recorded activity 2 — Verification intent: retention tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/log/test_retention.py`.

Historical expected evidence: FAIL because `RetentionManager` is missing.

**Recorded activity 3 — Design outcome: segment-granular retention**

```python
def apply(self, log: PartitionLog, retention_ms: int | None,
          retention_bytes: int | None) -> tuple[int, ...]:
    candidates = list(log.closed_segments)
    expired = {
        s.base_offset for s in candidates
        if retention_ms is not None
        and s.max_timestamp_ms < self.clock.now_ms() - retention_ms
    }
    total = log.size_bytes
    for segment in candidates:
        if retention_bytes is not None and total > retention_bytes:
            expired.add(segment.base_offset)
            total -= segment.size_bytes
    return log.delete_closed_segments(sorted(expired))
```

The design deleted log and index files only after installing a segment view that excludes
them; advance the start offset to the first remaining segment base.

**Recorded activity 4 — Verification intent: retention tests**

Historical verification covered targeted or full test coverage, including `tests/log/test_retention.py`.

Historical expected evidence: PASS.

### Milestone 11: Log compaction and tombstones

**Recorded file scope:**
- Added: `src/minikafka/log/compaction.py`
- Added: `tests/log/test_compaction.py`
- Added: `tests/reliability/test_compaction_swap.py`

**Recorded activity 1 — Test intent: failing latest-key, gap, tombstone, and swap tests**

```python
def test_compaction_preserves_latest_key_and_original_offsets(compactable_log) -> None:
    compactable_log.append_values(
        [(b"a", b"1"), (b"b", b"x"), (b"a", b"2"), (None, b"event")]
    )
    compactable_log.roll()
    LogCompactor(clock()).compact(compactable_log)
    records = compactable_log.fetch(0, 10)
    assert [(r.offset, r.key, r.value) for r in records] == [
        (1, b"b", b"x"),
        (2, b"a", b"2"),
        (3, None, b"event"),
    ]


def test_recent_tombstone_is_retained(compactable_log, clock) -> None:
    compactable_log.append_values([(b"a", b"1"), (b"a", None)])
    compactable_log.roll()
    LogCompactor(clock, delete_retention_ms=1000).compact(compactable_log)
    assert compactable_log.fetch(0, 10)[-1].value is None
```

**Recorded activity 2 — Verification intent: compaction tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/log/test_compaction.py`, `tests/reliability/test_compaction_swap.py`.

Historical expected evidence: FAIL because `LogCompactor` is missing.

**Recorded activity 3 — Design outcome: closed-segment cleaning and atomic replacement**

```python
def compact(self, log: PartitionLog) -> CompactionResult:
    source = tuple(log.closed_segments)
    latest = self._latest_offsets(log.segments)
    kept = tuple(
        record for record in self._records(source)
        if record.key is None
        or record.offset == latest[record.key]
        or self._retain_tombstone(record)
    )
    replacement = self._write_replacement(log, kept)
    replacement.flush()
    return log.install_compacted_segments(source, replacement)
```

Historical test and implementation coverage included replacement files under a unique sibling temporary directory, fsync
files and directory, rename into final names, atomically install the new view,
then unlink superseded files. Inject failures before install and prove the old
view remains authoritative.

**Recorded activity 4 — Verification intent: compaction tests**

Historical verification covered targeted or full test coverage, including `tests/log/test_compaction.py`, `tests/reliability/test_compaction_swap.py`.

Historical expected evidence: PASS.

### Milestone 12: Replica sets, follower fetch, ISR, and high watermark

**Recorded file scope:**
- Added: `src/minikafka/replication/__init__.py`
- Added: `src/minikafka/replication/model.py`
- Added: `src/minikafka/replication/replica.py`
- Added: `src/minikafka/replication/replica_set.py`
- Added: `tests/replication/test_follower_fetch.py`
- Added: `tests/replication/test_isr.py`
- Added: `tests/replication/test_high_watermark.py`

**Recorded activity 1 — Test intent: failing replication and visibility tests**

```python
@pytest.mark.asyncio
async def test_high_watermark_is_minimum_isr_leo(replica_set) -> None:
    result = await replica_set.append(unassigned_batch(b"x"), AckMode.LEADER)
    assert result.next_offset == 1
    assert replica_set.leader.leo == 1
    assert replica_set.high_watermark == 0
    await replica_set.fetch_followers_once()
    assert replica_set.high_watermark == 1


@pytest.mark.asyncio
async def test_consumer_cannot_read_above_high_watermark(replica_set) -> None:
    await replica_set.append(unassigned_batch(b"hidden"), AckMode.LEADER)
    assert await replica_set.fetch(0, 10, IsolationLevel.READ_COMMITTED) == ()
```

**Recorded activity 2 — Verification intent: replication tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/replication/test_follower_fetch.py`, `tests/replication/test_isr.py`, `tests/replication/test_high_watermark.py`.

Historical expected evidence: FAIL because replication components are missing.

**Recorded activity 3 — Design outcome: pull replication and exclusive HW**

```python
async def fetch_followers_once(self) -> None:
    async with self._lock:
        for replica in self.followers:
            batches = self.leader.log.read_batches(replica.leo, self.max_fetch_bytes)
            replica.append_replica_batches(batches, self.leader_epoch)
            replica.last_fetch_ms = self.clock.now_ms()
        self._refresh_isr()
        self._advance_high_watermark()


def _advance_high_watermark(self) -> None:
    candidate = min(self.replicas[broker_id].leo for broker_id in self.isr)
    self.high_watermark = max(self.high_watermark, candidate)
```

Follower append validates leader epoch and offset continuity. ISR refresh uses
configured time and offset lag thresholds. Fetch selects HW for normal
visibility and hides control records.

**Recorded activity 4 — Verification intent: replication tests**

Historical verification covered targeted or full test coverage, including `tests/replication`.

Historical expected evidence: PASS.

### Milestone 13: Ack modes and acknowledged-write loss

**Recorded file scope:**
- Changed: `src/minikafka/replication/replica_set.py`
- Changed: `src/minikafka/producer/producer.py`
- Added: `tests/replication/test_ack_modes.py`
- Added: `tests/reliability/test_lost_acked_write.py`

**Recorded activity 1 — Test intent: failing `acks=all`, insufficient ISR, and loss tests**

```python
@pytest.mark.asyncio
async def test_acks_all_waits_for_acknowledgement_set(replica_set) -> None:
    pending = asyncio.create_task(
        replica_set.append(unassigned_batch(b"safe"), AckMode.ALL)
    )
    await asyncio.sleep(0)
    assert not pending.done()
    await replica_set.fetch_followers_once()
    assert (await pending).next_offset == 1


@pytest.mark.asyncio
async def test_acks_all_rejects_insufficient_isr(replica_set) -> None:
    replica_set.remove_from_isr(replica_set.follower_ids[0])
    with pytest.raises(NotEnoughReplicas):
        await replica_set.append(unassigned_batch(b"x"), AckMode.ALL)
```

**Recorded activity 2 — Verification intent: ack tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/replication/test_ack_modes.py`, `tests/reliability/test_lost_acked_write.py`.

Historical expected evidence: FAIL because acknowledgement waiters are not implemented.

**Recorded activity 3 — Design outcome: acknowledgement snapshots and waiter resolution**

```python
async def append(self, batch: RecordBatch, acks: AckMode) -> ProduceResult:
    async with self._lock:
        if acks is AckMode.ALL and len(self.isr) < self.min_insync_replicas:
            raise NotEnoughReplicas()
        info = self.leader.log.append(batch)
        self._advance_high_watermark()
        if acks in (AckMode.NONE, AckMode.LEADER):
            return ProduceResult.from_append(info, known=acks is not AckMode.NONE)
        waiter = AckWaiter(info.next_offset, frozenset(self.isr))
        self._ack_waiters.append(waiter)
    return await waiter.future
```

Resolve only after every member of the captured acknowledgement set reaches
the batch end. If ISR drops below minimum first, fail with
`NotEnoughReplicasAfterAppend`. Add an explicit test-only reply-loss gate so
non-idempotent retry can observe an ambiguous outcome.

**Recorded activity 4 — Verification intent: ack and loss tests**

Historical verification covered targeted or full test coverage, including `tests/replication/test_ack_modes.py`, `tests/reliability/test_lost_acked_write.py`.

Historical expected evidence: PASS.

### Milestone 14: Leader epochs, promotion, and divergent-tail truncation

**Recorded file scope:**
- Changed: `src/minikafka/replication/replica_set.py`
- Changed: `src/minikafka/core/metadata.py`
- Added: `tests/replication/test_promotion.py`
- Added: `tests/replication/test_divergent_tail.py`

**Recorded activity 1 — Test intent: failing promotion/fencing/truncation tests**

```python
@pytest.mark.asyncio
async def test_only_isr_replica_can_be_promoted(replica_set) -> None:
    lagging = replica_set.follower_ids[0]
    replica_set.remove_from_isr(lagging)
    with pytest.raises(NotInSyncReplica):
        await replica_set.promote(lagging)


@pytest.mark.asyncio
async def test_old_leader_truncates_uncommitted_tail(replica_set) -> None:
    committed = asyncio.create_task(
        replica_set.append(unassigned_batch(b"committed"), AckMode.ALL)
    )
    await replica_set.fetch_followers_once()
    await committed
    await replica_set.append(unassigned_batch(b"leader-only"), AckMode.LEADER)
    old_leader = replica_set.leader_id
    await replica_set.promote(replica_set.follower_ids[0])
    await replica_set.rejoin(old_leader)
    assert replica_set.replicas[old_leader].leo == 1
```

**Recorded activity 2 — Verification intent: promotion tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/replication/test_promotion.py`, `tests/replication/test_divergent_tail.py`.

Historical expected evidence: FAIL because promotion/rejoin are missing.

**Recorded activity 3 — Design outcome: manual safe promotion**

```python
async def promote(self, broker_id: int) -> None:
    async with self._lock:
        if broker_id not in self.isr:
            raise NotInSyncReplica(broker_id)
        safe_end = self.high_watermark
        self.leader_epoch += 1
        self.leader_id = broker_id
        self.replicas[broker_id].log.truncate_to(safe_end)
        self.isr = {broker_id}
        self.high_watermark = safe_end
        self._fail_old_epoch_waiters()


async def rejoin(self, broker_id: int) -> None:
    replica = self.replicas[broker_id]
    replica.log.truncate_to(self.high_watermark)
    replica.mark_follower(self.leader_epoch)
```

Every produce/follower-fetch operation validates the supplied leader epoch.
Persist the updated leader and epoch in metadata after the data-plane barrier.

**Recorded activity 4 — Verification intent: promotion tests**

Historical verification covered targeted or full test coverage, including `tests/replication/test_promotion.py`, `tests/replication/test_divergent_tail.py`.

Historical expected evidence: PASS.

### Milestone 15: Idempotent producer state

**Recorded file scope:**
- Added: `src/minikafka/producer/state.py`
- Changed: `src/minikafka/producer/producer.py`
- Changed: `src/minikafka/replication/replica_set.py`
- Added: `tests/producer/test_idempotence.py`
- Added: `tests/reliability/test_producer_state_restart.py`

**Recorded activity 1 — Test intent: failing duplicate, gap, epoch-fence, and restart tests**

```python
@pytest.mark.asyncio
async def test_exact_retry_returns_original_offsets(replica_set) -> None:
    batch = sequenced_batch(producer_id=7, epoch=1, sequence=0, value=b"x")
    pending = asyncio.create_task(replica_set.append(batch, AckMode.ALL))
    await replica_set.fetch_followers_once()
    first = await pending
    second = await replica_set.append(batch, AckMode.ALL)
    assert second == first
    assert replica_set.leader.leo == 1


@pytest.mark.asyncio
async def test_old_producer_epoch_is_fenced(cluster) -> None:
    old = cluster.producer(transactional_name="writer", idempotent=True)
    new = cluster.producer(transactional_name="writer", idempotent=True)
    with pytest.raises(ProducerFenced):
        await old.send("events", value=b"stale")
```

**Recorded activity 2 — Verification intent: idempotence tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/producer/test_idempotence.py`, `tests/reliability/test_producer_state_restart.py`.

Historical expected evidence: FAIL because producer state validation is absent.

**Recorded activity 3 — Design outcome: sequence validation and state rebuild**

```python
def validate(self, batch: RecordBatch) -> DuplicateResult | None:
    key = (batch.producer_id, self.topic_partition)
    previous = self._states.get(key)
    if previous and batch.producer_epoch < previous.epoch:
        raise ProducerFenced(batch.producer_id)
    if previous and batch.base_sequence == previous.base_sequence:
        if batch.last_sequence == previous.last_sequence:
            return previous.result
        raise DuplicateSequence(batch.base_sequence)
    expected = 0 if previous is None or batch.producer_epoch > previous.epoch else (
        previous.last_sequence + 1
    )
    if batch.base_sequence != expected:
        raise OutOfOrderSequence(expected, batch.base_sequence)
    return None
```

The recorded change updated state only after successful append; store resulting offset ranges.
Rebuild by scanning retained batch headers on replica startup. Require
`AckMode.ALL` when constructing an idempotent producer.

**Recorded activity 4 — Verification intent: producer-state tests**

Historical verification covered targeted or full test coverage, including `tests/producer/test_idempotence.py`, `tests/reliability/test_producer_state_restart.py`.

Historical expected evidence: PASS.

### Milestone 16: Transaction markers and isolation

**Recorded file scope:**
- Added: `src/minikafka/transaction/__init__.py`
- Added: `src/minikafka/transaction/model.py`
- Added: `src/minikafka/transaction/journal.py`
- Added: `src/minikafka/transaction/manager.py`
- Changed: `src/minikafka/replication/replica_set.py`
- Added: `tests/transaction/test_visibility.py`
- Added: `tests/transaction/test_abort.py`

**Recorded activity 1 — Test intent: failing committed/uncommitted/aborted visibility tests**

```python
@pytest.mark.asyncio
async def test_read_committed_waits_for_commit_marker(cluster) -> None:
    tx = await cluster.transactions.begin("tx-1")
    await tx.send("out", value=b"pending")
    await cluster.replicate_all_once()
    assert await fetch_values(cluster, "out", READ_UNCOMMITTED) == [b"pending"]
    assert await fetch_values(cluster, "out", READ_COMMITTED) == []
    await tx.commit()
    await cluster.replicate_all_once()
    assert await fetch_values(cluster, "out", READ_COMMITTED) == [b"pending"]


@pytest.mark.asyncio
async def test_aborted_records_remain_hidden(cluster) -> None:
    tx = await cluster.transactions.begin("tx-2")
    await tx.send("out", value=b"discard")
    await tx.abort()
    await cluster.replicate_all_once()
    assert await fetch_values(cluster, "out", READ_COMMITTED) == []
```

**Recorded activity 2 — Verification intent: transaction visibility tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/transaction/test_visibility.py`, `tests/transaction/test_abort.py`.

Historical expected evidence: FAIL because transaction components are missing.

**Recorded activity 3 — Design outcome: transaction states, markers, and filtering**

```python
async def commit(self, transaction_id: str) -> None:
    tx = self._require_state(transaction_id, TransactionState.ONGOING)
    tx.state = TransactionState.PREPARE_COMMIT
    self.journal.append(tx)
    for tp in sorted(tx.partitions):
        await self.cluster.append_control(tp, transaction_id, ControlType.COMMIT)
    tx.state = TransactionState.COMPLETE_COMMIT
    self.journal.append(tx)


def visible_records(self, batches, isolation: IsolationLevel):
    for batch in batches:
        if batch.control is not None:
            continue
        if isolation is READ_UNCOMMITTED or batch.transactional_id is None:
            yield from batch.records
        elif self.transaction_status(batch.transactional_id) is COMMITTED:
            yield from batch.records
```

Track the first unstable offset per open transaction and cap
`read_committed` at the last stable offset. Control batches carry no user
records and are always hidden.

**Recorded activity 4 — Verification intent: transaction visibility tests**

Historical verification covered targeted or full test coverage, including `tests/transaction/test_visibility.py`, `tests/transaction/test_abort.py`.

Historical expected evidence: PASS.

### Milestone 17: Transactional offset commits and journal recovery

**Recorded file scope:**
- Changed: `src/minikafka/transaction/manager.py`
- Changed: `src/minikafka/transaction/journal.py`
- Changed: `src/minikafka/consumer/group.py`
- Added: `tests/transaction/test_offsets.py`
- Added: `tests/reliability/test_transaction_restart.py`

**Recorded activity 1 — Test intent: failing staged-offset atomicity and restart tests**

```python
@pytest.mark.asyncio
async def test_output_and_input_offset_publish_together(cluster) -> None:
    tx = await cluster.transactions.begin("processor")
    await tx.send("output", value=b"result")
    await tx.send_offsets("workers", {TopicPartition("input", 0): 4})
    assert await cluster.offsets.get("workers", TopicPartition("input", 0)) is None
    await tx.commit()
    assert await cluster.offsets.get("workers", TopicPartition("input", 0)) == 4
    assert await fetch_values(cluster, "output", READ_COMMITTED) == [b"result"]


@pytest.mark.asyncio
async def test_abort_discards_staged_offsets(cluster) -> None:
    tx = await cluster.transactions.begin("processor")
    await tx.send_offsets("workers", {TopicPartition("input", 0): 4})
    await tx.abort()
    assert await cluster.offsets.get("workers", TopicPartition("input", 0)) is None
```

**Recorded activity 2 — Verification intent: transactional-offset tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/transaction/test_offsets.py`, `tests/reliability/test_transaction_restart.py`.

Historical expected evidence: FAIL because staged offsets/recovery are missing.

**Recorded activity 3 — Publish offsets only at complete commit**

```python
async def _complete_commit(self, tx: Transaction) -> None:
    for tp in sorted(tx.partitions):
        await self.cluster.append_control(tp, tx.transaction_id, ControlType.COMMIT)
    await self.offset_store.commit_many(tx.staged_offsets)
    tx.state = TransactionState.COMPLETE_COMMIT
    self.journal.append(tx)
    self.journal.flush()
```

Journal every state transition as a checksummed frame. Recovery truncates an
incomplete journal tail, reconstructs transactions, finishes
`PREPARE_COMMIT` markers/offset publication, and completes
`PREPARE_ABORT` with abort markers. It fences unresolved old producer epochs.

**Recorded activity 4 — Verification intent: transaction recovery tests**

Historical verification covered targeted or full test coverage, including `tests/transaction`, `tests/reliability/test_transaction_restart.py`.

Historical expected evidence: PASS.

### Milestone 18: Thin JSON/TCP adapter

**Recorded file scope:**
- Added: `src/minikafka/adapters/json_tcp.py`
- Added: `tests/adapters/test_json_tcp.py`
- Added: `tests/adapters/test_direct_tcp_parity.py`

**Recorded activity 1 — Test intent: failing framing, base64, bounds, and parity tests**

```python
@pytest.mark.asyncio
async def test_tcp_produce_and_fetch_match_direct(cluster) -> None:
    server = await JsonTcpServer.start(cluster, "127.0.0.1", 0)
    reader, writer = await asyncio.open_connection(*server.address)
    await send_json(writer, {
        "operation": "produce",
        "topic": "events",
        "key_b64": base64.b64encode(b"k").decode(),
        "value_b64": base64.b64encode(b"v").decode(),
        "acks": "1",
    })
    produced = await read_json(reader)
    assert produced["offset"] == 0
    direct = await cluster.fetch(TopicPartition("events", 0), 0, 1)
    assert direct[0].value == b"v"
```

**Recorded activity 2 — Verification intent: adapter tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/adapters`.

Historical expected evidence: FAIL because JSON/TCP server is missing.

**Recorded activity 3 — Design outcome: bounded newline JSON translation**

```python
async def _serve(self, reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter) -> None:
    try:
        while line := await reader.readline():
            if len(line) > self.max_frame_bytes:
                await self._write_error(writer, FrameTooLarge())
                break
            request = json.loads(line)
            reply = await self.dispatch(request)
            writer.write(json.dumps(reply, separators=(",", ":")).encode() + b"\n")
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()
```

Support `create_topic`, `metadata`, `produce`, `fetch`, `join_group`,
`heartbeat`, and `commit_offsets`. Encode bytes only as base64 and map typed
domain errors to `{ok:false, code, message}`.

**Recorded activity 4 — Verification intent: adapter tests**

Historical verification covered targeted or full test coverage, including `tests/adapters`.

Historical expected evidence: PASS.

### Milestone 19: Lifecycle, crash injection, and experiments

**Recorded file scope:**
- Added: `src/minikafka/lifecycle.py`
- Added: `src/minikafka/labs/__init__.py`
- Added: `src/minikafka/labs/delivery_semantics.py`
- Added: `src/minikafka/labs/leader_failure.py`
- Added: `src/minikafka/labs/rebalance.py`
- Added: `src/minikafka/labs/compaction.py`
- Added: `src/minikafka/labs/zero_copy.py`
- Added: `tests/reliability/test_shutdown.py`
- Added: `tests/reliability/test_background_failure.py`

**Recorded activity 1 — Test intent: failing graceful/crash/background-failure tests**

```python
@pytest.mark.asyncio
async def test_graceful_close_flushes_and_owns_no_tasks(tmp_path: Path) -> None:
    cluster = await BrokerCluster.open(config(tmp_path))
    producer = cluster.producer(linger_ms=1000)
    pending = producer.send("events", value=b"flush-me")
    await cluster.close()
    assert (await pending).offset == 0
    assert cluster.owned_tasks == ()


@pytest.mark.asyncio
async def test_background_storage_failure_is_terminal(cluster) -> None:
    cluster.failure_injector.fail_next_flush(StorageError("disk"))
    await cluster.run_flush_cycle()
    assert cluster.state is LifecycleState.FAILED
    with pytest.raises(StorageError):
        await cluster.create_topic("later", 1, 1)
```

**Recorded activity 2 — Verification intent: lifecycle tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/reliability/test_shutdown.py`, `tests/reliability/test_background_failure.py`.

Historical expected evidence: FAIL because lifecycle/task ownership is incomplete.

**Recorded activity 3 — Design outcome: explicit shutdown states and lab entrypoints**

```python
async def close(self) -> None:
    if self.state in (LifecycleState.CLOSING, LifecycleState.CLOSED):
        return
    self.state = LifecycleState.CLOSING
    await asyncio.gather(*(p.flush() for p in self._producers))
    await self._fail_or_resolve_waiters()
    await self._offset_store.flush()
    await self._transactions.flush()
    await self._flush_logs()
    await self._cancel_owned_tasks()
    self._close_files()
    self.state = LifecycleState.CLOSED
```

`crash()` cancels tasks and closes descriptors without draining or flushing.
Labs expose functions returning structured observations; `zero_copy.py`
compares `read/sendall` and `os.sendfile` when supported without pass/fail
throughput thresholds.

**Recorded activity 4 — Verification intent: lifecycle tests and compile all modules**

Historical verification covered targeted or full test coverage, bytecode compilation, including `tests/reliability/test_shutdown.py`, `tests/reliability/test_background_failure.py`.

Historical expected evidence: PASS with no compile errors.

### Milestone 20: Final acceptance, behavior matrix, and documentation

**Recorded file scope:**
- Added: `README.md`
- Added: `docs/behavior-matrix.md`
- Added: `tools/__init__.py`
- Added: `tools/count_sloc.py`
- Added: `tests/test_final_acceptance.py`
- Added: `tests/test_sloc_report.py`
- Changed: `src/minikafka/__init__.py`

**Recorded activity 1 — Test intent: the failing acceptance and documentation tests**

```python
@pytest.mark.asyncio
async def test_domain_closure_survives_rebalance_failover_and_restart(tmp_path: Path):
    async with BrokerCluster.open(config(tmp_path, broker_ids=(1, 2))) as cluster:
        await cluster.create_topic("orders", 2, 2)
        producer = cluster.producer(acks="all", idempotent=True)
        pending = producer.send("orders", key=b"user-42", value=b"created")
        await cluster.replicate_all_once()
        metadata = await pending
        first = cluster.consumer(group_id="workers")
        await first.subscribe(("orders",))
        records = await first.poll(10)
        assert records[0].value == b"created"
        await first.commit()
        await cluster.expire_consumer(first.member_id)
        second = cluster.consumer(group_id="workers")
        await second.subscribe(("orders",))
        await cluster.promote(
            TopicPartition("orders", metadata.partition), broker_id=2
        )
    async with BrokerCluster.open(config(tmp_path, broker_ids=(1, 2))) as reopened:
        assert await reopened.committed(
            "workers", TopicPartition("orders", metadata.partition)
        ) == records[-1].offset + 1


def test_readme_states_adapter_and_course_boundaries() -> None:
    text = Path("README.md").read_text()
    assert "not Kafka wire-protocol compatible" in text
    assert "Course material is separate" in text
```

**Recorded activity 2 — Verification intent: acceptance tests and verify RED**

Historical verification covered targeted or full test coverage, including `tests/test_final_acceptance.py`, `tests/test_sloc_report.py`, `tests/test_project_contract.py`.

Historical expected evidence: FAIL until exports, README, behavior evidence, and SLOC report exist.

**Recorded activity 3 — Finish public API and evidence documents**

The public interface exported:

```python
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.core.record import Header, Record, StoredRecord
from minikafka.replication.model import AckMode, IsolationLevel, RecordMetadata

__all__ = (
    "AckMode",
    "BrokerCluster",
    "Header",
    "IsolationLevel",
    "MiniKafkaConfig",
    "Record",
    "RecordMetadata",
    "StoredRecord",
    "TopicPartition",
)
```

README includes Direct quick start, JSON/TCP demonstration, semantic
simplifications, non-goals, official Apache Kafka source links, deterministic
failure experiments, test commands, and explicit course separation.
`behavior-matrix.md` maps every design acceptance criterion to a concrete test
node. SLOC tooling reports production/test/docs counts without a size gate.

**Recorded activity 4 — Verification intent: full verification**

Historical verification covered targeted or full test coverage, static analysis, bytecode compilation, diff hygiene, including `tools/count_sloc.py`.

Historical expected evidence: all tests pass, Ruff and compileall are clean, SLOC prints counts,
and the whitespace-error check emits no output.

## Plan self-review

- **Spec coverage:** Tasks 1-5 cover durable records/log/index/recovery; 6-9
  cover Direct metadata, producer, consumer, and group ownership; 10-11 cover
  retention/compaction; 12-14 cover follower fetch, ISR/HW, acknowledgements,
  epochs, promotion, and truncation; 15 covers idempotence; 16-17 cover
  transaction visibility, offsets, and bounded recovery; 18-20 cover adapter
  parity, lifecycle, experiments, documentation, and final acceptance.
- **Boundary consistency:** LEO, HW, positions, and committed offsets are
  exclusive next-offsets throughout. `read_committed` is used for transaction
  isolation and never changes the HW rule.
- **Naming consistency:** `BrokerCluster`, `PartitionLog`, `RecordBatch`,
  `TopicPartition`, `AckMode`, `IsolationLevel`, `GroupCoordinator`, and
  `TransactionManager` retain the same names across tasks.
- **Scope:** Kafka wire compatibility, KRaft, automatic election, full
  transaction coordination, and course content remain outside every task.
