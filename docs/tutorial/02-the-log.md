# Chapter 2 · The Log: Foundation of Everything

Kafka is often introduced as a message broker, but its durable abstraction is
an ordered, append-only partition log. Producers append; replicas copy the same
ordered batches; consumers name an offset and replay. MiniKafka makes that
foundation inspectable through three layers: `PartitionLog` owns a partition,
`Segment` owns a pair of files, and `OffsetIndex` narrows the starting point for
a scan.

## Learning objectives

After this chapter, you will be able to:

1. explain why a partition log is split into closed and active segments;
2. trace offset assignment, size-based rolling, and sparse-index lookup;
3. describe what the batch CRC protects;
4. distinguish repairable active-tail damage from fatal historical
   corruption; and
5. reproduce rolling, index rebuild, and tail truncation locally.

## From a logical log to segment files

`PartitionLog.open` in `src/minikafka/log/partition_log.py` scans the partition
directory for `*.log` files whose names are numeric base offsets. With no files,
it creates a segment at offset 0. Otherwise it sorts the bases and opens every
segment. All but the last are **closed**; the last is **active**. That distinction
drives both recovery and maintenance: historical closed segments must not
change, while an interrupted append may leave incomplete bytes at the active
tail.

Every segment has a twenty-digit stem. `segment_stem` in
`src/minikafka/log/segment.py` maps base offset 2 to
`00000000000000000002`, producing:

```text
00000000000000000002.log
00000000000000000002.index
```

The `.log` file contains complete encoded batches. The `.index` file contains
fixed-width `(relative offset, byte position)` entries. The base offset in the
filename plus a relative index entry recovers an absolute offset. A fixed-width
index is simple to validate and binary-search.

### Append and offset ownership

Clients do not assign storage offsets. `PartitionLog.append` calls
`RecordBatch.assign(self.leo, self.leader_epoch)` and then
`PartitionLog.append_replica_batch`. The latter requires the incoming batch's
base offset to equal the current LEO. This division matters: a leader assigns;
a follower appends the already assigned batch and verifies continuity.

Before writing, `append_replica_batch` encodes the batch to know its size. If
the active segment already contains data and the new frame would make
`active.size_bytes + encoded_size` exceed
`MiniKafkaConfig.segment_max_bytes`, it calls `PartitionLog._roll`. Rolling
flushes the old active segment and creates a new one whose base is the current
LEO. A single oversized batch is still accepted into an empty segment, so a
configured segment size is a rolling threshold, not a hard maximum frame size.

`Segment.append` in `src/minikafka/log/segment.py` writes the encoded frame at
the file end and advances `Segment.leo` to `batch.next_offset`. It adds an index
entry for the first batch and thereafter only when the byte distance from the
last indexed position reaches `index_interval_bytes`. That is why the index is
**sparse**: it points near the answer rather than storing one row per record.

### Sparse lookup still scans

`OffsetIndex.floor_position` in `src/minikafka/log/index.py` uses
`bisect_right` to find the greatest indexed relative offset not greater than the
requested offset. `Segment.iter_batches` seeks to that byte position and
decodes forward until it finds batches whose `next_offset` crosses the request.
`Segment.scan` then expands records and filters their exact offsets.

Sparse indexing trades a small bounded scan for a much smaller index. It does
not change the log's source of truth: deleting an index must not delete data.
At startup, `Segment.open` scans the log and recreates the index from the valid
batches. The test `test_missing_index_is_rebuilt_from_log` in
`tests/log/test_recovery.py` makes this contract executable.

### Frame structure and CRC

`encode_batch` in `src/minikafka/core/batch_codec.py` writes a frame header
containing:

```text
magic ("MKB1") | payload length | CRC32(payload)
```

The payload carries the format version, base offset, leader epoch, producer
identity and sequence, transaction flags, offset deltas, and records. Keys,
values, and header values use signed lengths so `None` differs from empty
bytes. `decode_batch` checks magic, maximum length, exact frame length, CRC,
format version, flags, record fields, and trailing bytes before constructing a
`RecordBatch`.

CRC detects accidental corruption; it is not cryptographic authentication.
The checksum says that stored bytes match the frame that was written, not that
a trusted actor wrote them.

## Startup recovery: repair only the active tail

`Segment.open` reads frames from byte position zero. It also checks that batch
offsets do not overlap or move backwards. If decoding fails:

- for a closed segment, it closes the file and raises `StorageError`;
- for the active segment, it truncates to the byte position immediately after
  the last valid batch and continues opening.

This is a fail-closed policy for history and a bounded repair policy for the
only location where an interrupted append is expected. Silently discarding a
corrupt middle segment would create an unexplained hole. Refusing startup makes
the damage explicit.

After the scan, `Segment.open` deletes any existing index and rebuilds it.
`PartitionLog.open` also scans stored batches to recover the maximum leader
epoch. The wrapper `recover_active_segment` in
`src/minikafka/log/recovery.py` is deliberately thin; the actual recovery
mechanism lives in `Segment.open`.

`PartitionLog.truncate_to` is different from startup tail repair. Replication
uses it at a known batch boundary to discard a divergent suffix. MiniKafka
collects retained batches, removes all segment files, creates a new segment,
and re-appends the retained batches. Chapter 6 connects this operation to
promotion and leader epochs.

## MiniKafka versus Apache Kafka

Real Kafka also stores partitions as ordered segment files with offset indexes,
rolls by configured size or time, validates record-batch CRCs, and recovers log
tails. Relevant broker settings include `log.segment.bytes`,
`log.segment.ms`, and `log.index.interval.bytes`.

The representation is intentionally different. MiniKafka uses the custom
uncompressed `MKB1` frame and an eight/twelve-byte teaching index format; it
does not implement Kafka's record batch wire layout, compression codecs, time
index, transaction index, or memory-mapped index machinery. It rolls only by
size, not time. Startup scans every stored batch to recover the maximum leader
epoch, whereas Kafka maintains leader-epoch checkpoint data.

MiniKafka's `PartitionLog.truncate_to` rewrites the retained log into one new
segment. Kafka normally truncates affected tail files and indexes in place.
These differences are listed in the
[behavior matrix](../behavior-matrix.md) and explained in the
[Kafka mapping](../kafka-mapping.md). The shared invariant is ordered complete
batches and an explicit valid boundary—not file-format compatibility.

## Hands-on experiment: rolling and repairing the tail

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run --offline python - <<'PY'
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.log.partition_log import PartitionLog

def one(value: bytes, timestamp: int) -> RecordBatch:
    return RecordBatch.unassigned((Record(None, value, timestamp),))

with TemporaryDirectory() as directory:
    root = Path(directory)
    config = MiniKafkaConfig(
        root, segment_max_bytes=120, index_interval_bytes=1
    )
    log = PartitionLog.open(root / "events-0", config)
    for i in range(3):
        log.append(one(f"value-{i}".encode(), i))
    print("segment bases:", [s.base_offset for s in log.segments])
    print("active index:", log.active.index.entries)
    active_path = log.active.log_path
    valid_size = log.active.size_bytes
    log.close()
    with active_path.open("ab") as file:
        file.write(b"MKB1\x00\x00\x00\x20partial")
    recovered = PartitionLog.open(root / "events-0", config)
    print("recovered leo:", recovered.leo)
    print("tail repaired:", recovered.active.size_bytes == valid_size)
    print("values:", [r.value for r in recovered.fetch(0, 10)])
    recovered.close()
PY
```

Measured output:

```text
segment bases: [0, 1, 2]
active index: ((2, 0),)
recovered leo: 3
tail repaired: True
values: [b'value-0', b'value-1', b'value-2']
```

The deliberately tiny segment threshold makes each batch roll into a new
segment. The active segment begins at offset 2, so its first sparse-index entry
maps absolute offset 2 to byte position 0. Appending an incomplete frame
simulates a process dying mid-write. Reopening recognizes the last file as
active, truncates only the invalid suffix, rebuilds its index, and preserves LEO
3.

Do not generalize `tail repaired: True` into “all corruption is repaired.”
Flip a byte in the first closed `.log` instead and startup raises
`StorageError`. That refusal protects the historical prefix.

## Exercises

### 1. Understanding: why the first oversized batch is accepted

Explain why `segment_max_bytes` is not a strict maximum batch size in
`PartitionLog.append_replica_batch`.

Acceptance: cite both boolean conditions that guard `_roll` and describe the
otherwise-impossible failure mode avoided by accepting one batch.

??? note "Reference answer"

    Rolling occurs only when `active.has_data` is true **and** adding the encoded
    frame would exceed the threshold. An empty segment accepts an oversized
    batch. If it rolled first, the new segment would also be empty and the same
    decision could repeat forever. Kafka similarly treats segment size as a
    rolling target that can be exceeded by one batch.

### 2. Hands-on: prove index rebuild

Extend the inline script to delete the active `.index` after closing the log,
then reopen and print `recovered.active.index.entries`.

Acceptance: reopening succeeds, the fetched values are unchanged, the index
file exists again, and `git diff -- src` is empty.

??? note "Reference answer"

    Before reopening:

    ```python
    index_path = active_path.with_suffix(".index")
    index_path.unlink()
    ```

    `Segment.open` scans valid log frames and calls `OffsetIndex.create`, so the
    index is derived data. With `index_interval_bytes=1`, the rebuilt active
    index is `((2, 0),)`.

### 3. Code-change design: add a time index

Draft a patch plan, without changing `src/`, for a sparse time index that
supports “first offset at or after timestamp.” Identify at least three places
that must change and one crash-recovery test.

Acceptance: the plan covers file lifecycle, append, rebuild, and closed-segment
failure behavior; adding only a lookup function is insufficient.

??? note "Reference answer"

    A complete change needs a `TimeIndex` file format; creation/open/flush/close
    integration in `Segment`; append entries based on `batch.max_timestamp_ms`;
    startup rebuild beside the offset index; deletion, compaction swap, and
    truncation lifecycle updates in `PartitionLog`; and a query that searches
    segment maxima before scanning. A recovery test should delete the active
    time index, reopen, and confirm timestamp lookup returns the same offset.
    A closed corrupt time index needs an explicit policy rather than silent
    ambiguity.

## Summary

The partition log is an ordered sequence of complete CRC-protected batches.
Segments bound maintenance work, sparse indexes accelerate lookup without
becoming the source of truth, and startup repair stops exactly at the last
valid active-tail boundary. Those mechanics explain how records remain
replayable after restart. Chapter 3 builds on the same segment boundaries to
remove old prefixes and compact obsolete keyed values while preserving
meaningful offset gaps.
