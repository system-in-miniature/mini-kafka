# Chapter 3 · Retention and Compaction

An append-only log cannot grow forever. Kafka offers two complementary cleanup
ideas: retention discards old segment prefixes, while compaction discards
obsolete values for keys. They answer different questions. Retention asks
“which old history may disappear?” Compaction asks “which record is the latest
state for this key?” MiniKafka keeps the distinction visible and makes every
rewrite operate on closed segments.

## Learning objectives

After this chapter, you will be able to:

1. distinguish time/size retention from keyed log compaction;
2. explain why retention deletes only a continuous prefix of closed segments;
3. derive which keyed, keyless, and tombstone records survive compaction;
4. explain how original offsets and gaps survive a rewrite; and
5. evaluate the guarantees and crash gap of MiniKafka's two-directory swap.

## Retention removes prefixes, not arbitrary records

`RetentionManager.apply` in `src/minikafka/log/retention.py` receives a
`PartitionLog` plus optional time and size budgets. Negative time and
non-positive byte budgets are rejected. The manager considers only
`log.closed_segments`; the active segment remains writable and is never
selected.

For time retention, it computes:

```python
boundary = clock.now_ms() - retention_ms
```

It walks closed segments from oldest to newest and selects a segment while
`segment.max_timestamp_ms < boundary`. It stops at the first segment that is
not expired. This stop is essential. Even if a later segment happens to carry
an older timestamp, deleting it would create a middle hole in the physical log.
Kafka retention likewise works with eligible segment prefixes rather than
plucking individual records from history.

Size retention starts after anything already selected by time. It calculates
the bytes that would remain and keeps selecting oldest closed segments until
the total fits `retention_bytes` or no more closed segments exist. Combining
time and size therefore produces one prefix: time can select an initial part,
and size can extend that selection.

`PartitionLog.delete_closed_segments` in
`src/minikafka/log/partition_log.py` enforces the policy again. It rejects an
active or unknown base offset and compares the requested set with the first
`len(requested)` closed segments. Only an exact continuous prefix is accepted.
It then closes those segment handles and removes their `.log` and `.index`
files. The new `log_start_offset` is the base offset of the first surviving
segment. Fetching an older offset raises `OffsetOutOfRange`; old offsets are not
renumbered.

### Time semantics are segment semantics

A segment is eligible only when its **maximum** record timestamp is old enough.
One recent record keeps the whole segment. This makes cleanup coarse but safe:
the broker does not rewrite a segment merely to enforce retention. Segment
size and rolling cadence therefore influence how promptly expired data can be
removed.

MiniKafka stores `Segment.max_timestamp_ms` while appending and reconstructs it
while opening. It has no separate time index. Consequently it can decide
segment eligibility but does not support Kafka's timestamp-to-offset lookup.

## Compaction keeps the latest value per key

`LogCompactor.compact` in `src/minikafka/log/compaction.py` snapshots the closed
segments as its source. With no closed segments, it returns an empty
`CompactionResult`. Otherwise `_latest_offsets` scans **all** segments,
including the active segment, and records the greatest offset for every
non-null key.

Including the active segment in the latest-offset map is subtle. Suppose a
closed segment contains `a=old` and the active segment contains `a=new`.
Although the active segment is not rewritten, the closed value is obsolete and
must disappear. At the same time, all active batches are copied unchanged into
the replacement directory.

`LogCompactor._compact_batch` applies three rules:

1. a record with `key is None` is retained because there is no key-based state
   identity;
2. a keyed record is retained only if its absolute offset equals the latest
   offset recorded for that key;
3. a latest keyed tombstone (`value is None`) is retained until its age is
   greater than `delete_retention_ms`, then it too may disappear.

A tombstone is not “an empty value.” It is a deletion marker. Keeping it for a
grace period gives downstream replicas or consumers time to observe that the
key was deleted. Once the tombstone expires, both older values and the marker
can be absent from the compacted view.

Control batches pass through unchanged. When only some records in a batch
survive, `dataclasses.replace` retains the batch's base offset and
`last_offset_delta` but replaces `records` and `offset_deltas`. Thus a batch may
physically contain offsets 1 and 3 while its logical range still ends at 4.
Offset gaps are expected after compaction. Consumers must advance according to
stored offsets, not assume `number of returned records == offset distance`.

## Building and installing the compacted directory

`PartitionLog.install_compacted_closed` builds a sibling temporary directory.
It creates one compacted segment at the current `log_start_offset`, appends
rewritten closed batches with `allow_gap=True`, and then copies the active
segment unchanged. Both new segments are flushed and closed before publication.

Publication uses two `os.replace` calls:

1. live directory → uniquely named backup;
2. temporary directory → live directory.

The parent directory is then `fsync`ed, the replacement log is reopened, and
the backup is removed. If the second rename raises inside the process, the
first rename is rolled back. If the injectable `before_swap` hook raises, the
live directory has not moved and temporary data is cleaned up.

The pair of renames is **not one atomic exchange**. A process or machine crash
between them can leave the backup directory without a live directory. The code
comment in `LogCompactor.compact` states this explicitly. The method prevents
partially built files from becoming live and handles in-process exceptions, but
it is not a complete crash-recovery protocol for every power-loss point.

## MiniKafka versus Apache Kafka

Real Kafka exposes topic policies such as `cleanup.policy=delete`,
`cleanup.policy=compact`, or both. Important settings include
`retention.ms`, `retention.bytes`, `segment.bytes`, and
`delete.retention.ms`. Its log cleaner selects eligible dirty segments,
rewrites them in the background, and uses a richer file lifecycle with cleaner
checkpoints. Production Kafka also accounts for transaction markers, lag,
cleanable ratios, and multiple index types.

MiniKafka performs cleanup only when the caller invokes `RetentionManager` or
`LogCompactor`; there is no background cleaner. Its compactor rebuilds the
entire closed-segment view into a sibling directory, and the rename pair has
the crash window described above. Time retention uses segment maxima without a
time index. The semantic evidence and limitations are linked in the
[behavior matrix](../behavior-matrix.md) and
[Kafka mapping](../kafka-mapping.md).

The key shared ideas are still worth studying: deletion respects segment
boundaries, compaction is key-based rather than age-based, tombstones have a
retention window, and offsets are stable even when records disappear.

## Hands-on experiment: delete a prefix, compact keyed state

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run --offline python - <<'PY'
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.log.compaction import LogCompactor
from minikafka.log.partition_log import PartitionLog
from minikafka.log.retention import RetentionManager

def append(log, *items):
    log.append(RecordBatch.unassigned(
        tuple(Record(k, v, ts) for k, v, ts in items)
    ))

with TemporaryDirectory() as directory:
    root = Path(directory)
    config = MiniKafkaConfig(root, index_interval_bytes=1)
    retained = PartitionLog.open(root / "retained", config)
    for ts in (0, 50, 150):
        append(retained, (None, str(ts).encode(), ts))
        retained.roll()
    append(retained, (None, b"250", 250))
    deleted = RetentionManager(ManualClock(300)).apply(
        retained, retention_ms=100, retention_bytes=None
    )
    print("retention deleted:", deleted, "start:", retained.log_start_offset)
    retained.close()

    compacted = PartitionLog.open(root / "compacted", config)
    append(compacted, (b"a", b"1", 0), (b"b", b"x", 1))
    compacted.roll()
    append(compacted, (b"a", b"2", 2), (None, b"event", 3))
    compacted.roll()
    append(compacted, (b"active", b"keep", 4))
    result = LogCompactor(
        ManualClock(10), delete_retention_ms=1000
    ).compact(compacted)
    print("compaction:", result.records_before, "->", result.records_after)
    print("records:", [
        (r.offset, r.key, r.value) for r in compacted.fetch(0, 10)
    ])
    compacted.close()
PY
```

Measured output:

```text
retention deleted: (0, 1, 2) start: 3
compaction: 4 -> 3
records: [(1, b'b', b'x'), (2, b'a', b'2'), (3, None, b'event'), (4, b'active', b'keep')]
```

At clock 300 with a 100 ms window, the three closed segments have maximum
timestamps below 200, so the prefix bases 0, 1, and 2 disappears. The active
segment at base 3 remains.

Compaction examines four closed records. `a=1` at offset 0 is obsolete;
`b=x`, latest `a=2`, and the keyless event survive. The active record is copied
but is not included in the `4 -> 3` closed-record count. The missing offset 0 is
observable evidence that compaction preserves addresses rather than
renumbering the log.

## Exercises

### 1. Understanding: non-monotonic timestamps

Closed segment maxima are 250, 0, and 0; the clock is 300 and
`retention_ms=100`. Which segments may time retention delete?

Acceptance: your answer follows the exact loop in `RetentionManager.apply` and
mentions the prefix invariant.

??? note "Reference answer"

    None. The boundary is 200, and the oldest closed segment has maximum
    timestamp 250, so the loop stops immediately. It must not skip that segment
    and delete later ones, because retention may delete only a continuous
    prefix.

### 2. Hands-on: observe tombstone expiry

Build a closed segment containing `a=1` at timestamp 0 and `a=None` at
timestamp 100. Compact once at clock 500 and once in a fresh log at clock 2000,
both with `delete_retention_ms=1000`.

Acceptance: the first result contains the tombstone at its original offset; the
second contains neither value nor tombstone. Confirm `git diff -- src` is empty.

??? note "Reference answer"

    At clock 500 the tombstone age is 400, so `_compact_batch` keeps the latest
    keyed record with `value=None`. At clock 2000 its age is 1900, so it is
    dropped. The older value is never restored because it is not the latest
    offset in either run.

### 3. Code-change design: recover the rename crash window

Draft a recovery protocol for `install_compacted_closed` without applying it.
Use the live, backup, and temporary directory names as evidence.

Acceptance: define startup decisions for every combination of live/backup/temp
existence and identify when a directory must be validated before selection.

??? note "Reference answer"

    One design writes and fsyncs a small phase manifest before renames. Startup
    validates complete segment/index pairs in live and backup. If live is
    valid, it is authoritative and stale backup/temp can be quarantined. If
    live is missing but a valid backup exists, restore backup. A valid temp may
    be promoted only when the manifest proves the old live was already backed
    up and all new files were fsynced. Ambiguous or doubly invalid states must
    fail closed. Directory removal happens only after the selected live tree
    and parent directory are durable.

## Summary

Retention and compaction both make records disappear, but preserve different
contracts. Retention removes an eligible closed-segment prefix and advances the
log start. Compaction keeps latest keyed state, retains keyless events, ages out
tombstones, and preserves offset gaps. MiniKafka's sibling-directory rewrite
makes publication inspectable while honestly exposing a two-rename crash
window. Chapter 4 moves up the write path to see how producers select
partitions, form batches, and prevent retries from appending duplicates.
