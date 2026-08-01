# Stage 10 · Prefix-only retention

### Goal

Build prefix-only retention and explain its boundary from executable failure, runtime state, and the critical statement.

??? note "Deliverable files"
    - `src/minikafka/log/partition_log.py`
    - `src/minikafka/log/retention.py`
    - `src/minikafka/log/segment.py`
    - `tests/log/test_retention.py`

### The problem at this point

Disk limits require deletion, but removing arbitrary bytes or the active segment would break offset continuity and recovery.

### Test contract

#### See the failure first

Time and size tests create several segments and prove deletion removes only eligible closed prefixes while preserving the active tail.

??? note "File diff: tests/log/test_retention.py"
    ```diff
    diff --git a/tests/log/test_retention.py b/tests/log/test_retention.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..d74962771fbc55472d2483f057566972891cbf3a
    --- /dev/null
    +++ b/tests/log/test_retention.py
    @@ -0,0 +1,84 @@
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.batch import RecordBatch
    +from minikafka.core.record import Record
    +from minikafka.errors import OffsetOutOfRange
    +from minikafka.log.partition_log import PartitionLog
    +from minikafka.log.retention import RetentionManager
    +
    +
    +def append(log: PartitionLog, value: bytes, timestamp_ms: int) -> None:
    +    log.append(
    +        RecordBatch.unassigned(
    +            (Record(key=None, value=value, timestamp_ms=timestamp_ms),)
    +        )
    +    )
    +
    +
    +def rolled_log(tmp_path: Path) -> PartitionLog:
    +    config = MiniKafkaConfig(
    +        data_dir=tmp_path,
    +        segment_max_bytes=80,
    +        index_interval_bytes=1,
    +    )
    +    log = PartitionLog.open(tmp_path / "events-0", config)
    +    for timestamp in (0, 50, 150, 250):
    +        append(log, bytes([timestamp % 256]) * 20, timestamp)
    +    assert len(log.closed_segments) >= 3
    +    return log
    +
    +
    +def test_time_retention_deletes_closed_segments_only(tmp_path: Path) -> None:
    +    log = rolled_log(tmp_path)
    +    active_base = log.active.base_offset
    +    manager = RetentionManager(ManualClock(300))
    +
    +    deleted = manager.apply(log, retention_ms=100, retention_bytes=None)
    +
    +    assert deleted == (0, 1, 2)
    +    assert log.active.base_offset == active_base
    +    assert log.log_start_offset == active_base
    +    with pytest.raises(OffsetOutOfRange):
    +        log.fetch(0, 1)
    +    log.close()
    +
    +
    +def test_size_retention_deletes_oldest_segments_first(tmp_path: Path) -> None:
    +    log = rolled_log(tmp_path)
    +    newest_closed = log.closed_segments[-1].base_offset
    +    target = log.active.size_bytes + log.closed_segments[-1].size_bytes + 20
    +
    +    deleted = RetentionManager(ManualClock()).apply(
    +        log,
    +        retention_ms=None,
    +        retention_bytes=target,
    +    )
    +
    +    assert deleted
    +    assert deleted == tuple(sorted(deleted))
    +    assert newest_closed not in deleted
    +    assert log.log_start_offset > 0
    +    log.close()
    +
    +
    +def test_retention_result_survives_restart(tmp_path: Path) -> None:
    +    log = rolled_log(tmp_path)
    +    directory = log.directory
    +    config = log.config
    +    RetentionManager(ManualClock(300)).apply(
    +        log,
    +        retention_ms=100,
    +        retention_bytes=None,
    +    )
    +    start = log.log_start_offset
    +    log.close()
    +
    +    reopened = PartitionLog.open(directory, config)
    +
    +    assert reopened.log_start_offset == start
    +    assert [record.offset for record in reopened.fetch(start, 10)] == [start]
    +    reopened.close()
    ```

**What this test locks**

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

**How it constructs the counterexample**

Time and size tests create several segments and prove deletion removes only eligible closed prefixes while preserving the active tail.

**Key test statement**

```python
assert len(log.closed_segments) >= 3
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

**What a failure means**

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

Retention is physical deletion of old closed segments. Log start offset advances, while existing record offsets never change. The active segment is not a deletion candidate.

### Why this mechanism is necessary

Disk limits require deletion, but removing arbitrary bytes or the active segment would break offset continuity and recovery. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

The manager evaluates age or total bytes from oldest to newest, stops at the first ineligible boundary, deletes selected files, and refreshes the partition view.

### Mechanism blocks

#### Prefix-only retention mechanism

The manager evaluates age or total bytes from oldest to newest, stops at the first ineligible boundary, deletes selected files, and refreshes the partition view.

??? note "File diff: src/minikafka/log/partition_log.py"
    ```diff
    diff --git a/src/minikafka/log/partition_log.py b/src/minikafka/log/partition_log.py
    index e6d7a33c0adeb05f364797901415e2180653fa26..f55c17f40aedcd11ca2f0b3faf00a4424cb2ebe6 100644
    --- a/src/minikafka/log/partition_log.py
    +++ b/src/minikafka/log/partition_log.py
    @@ -99,10 +99,7 @@ class PartitionLog:

         @property
         def size_bytes(self) -> int:
    -        return sum(
    -            segment.size_bytes + segment.index_path.stat().st_size
    -            for segment in self._segments
    -        )
    +        return sum(segment.total_size_bytes for segment in self._segments)

         def append(self, batch: RecordBatch) -> LocatedBatch:
             assigned = batch.assign(self.leo, self.leader_epoch)
    @@ -235,6 +232,36 @@ class PartitionLog:
             for path in paths:
                 path.unlink(missing_ok=True)

    +    def delete_closed_segments(
    +        self,
    +        base_offsets: list[int] | tuple[int, ...],
    +    ) -> tuple[int, ...]:
    +        requested = set(base_offsets)
    +        closed = {segment.base_offset for segment in self.closed_segments}
    +        unknown = requested.difference(closed)
    +        if unknown:
    +            raise ValueError(
    +                f"retention can delete closed segments only: {sorted(unknown)}"
    +            )
    +        if not requested:
    +            return ()
    +        removed = [
    +            segment
    +            for segment in self._segments[:-1]
    +            if segment.base_offset in requested
    +        ]
    +        self._segments = [
    +            segment
    +            for segment in self._segments
    +            if segment.base_offset not in requested
    +        ]
    +        for segment in removed:
    +            paths = (segment.log_path, segment.index_path)
    +            segment.close()
    +            for path in paths:
    +                path.unlink(missing_ok=True)
    +        return tuple(sorted(requested))
    +
         def flush(self) -> None:
             for segment in self._segments:
                 segment.flush()
    ```

??? note "File diff: src/minikafka/log/retention.py"
    ```diff
    diff --git a/src/minikafka/log/retention.py b/src/minikafka/log/retention.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..0a77d0b2236bfef6e9a8d74807f234d18927789f
    --- /dev/null
    +++ b/src/minikafka/log/retention.py
    @@ -0,0 +1,47 @@
    +from __future__ import annotations
    +
    +from minikafka.clock import Clock
    +from minikafka.log.partition_log import PartitionLog
    +
    +
    +class RetentionManager:
    +    def __init__(self, clock: Clock) -> None:
    +        self.clock = clock
    +
    +    def apply(
    +        self,
    +        log: PartitionLog,
    +        *,
    +        retention_ms: int | None,
    +        retention_bytes: int | None,
    +    ) -> tuple[int, ...]:
    +        if retention_ms is not None and retention_ms < 0:
    +            raise ValueError("retention_ms cannot be negative")
    +        if retention_bytes is not None and retention_bytes <= 0:
    +            raise ValueError("retention_bytes must be positive")
    +
    +        closed = list(log.closed_segments)
    +        selected: set[int] = set()
    +        if retention_ms is not None:
    +            boundary = self.clock.now_ms() - retention_ms
    +            selected.update(
    +                segment.base_offset
    +                for segment in closed
    +                if segment.max_timestamp_ms < boundary
    +            )
    +
    +        if retention_bytes is not None:
    +            total = log.size_bytes - sum(
    +                segment.total_size_bytes
    +                for segment in closed
    +                if segment.base_offset in selected
    +            )
    +            for segment in closed:
    +                if total <= retention_bytes:
    +                    break
    +                if segment.base_offset in selected:
    +                    continue
    +                selected.add(segment.base_offset)
    +                total -= segment.total_size_bytes
    +
    +        return log.delete_closed_segments(tuple(sorted(selected)))
    ```

??? note "File diff: src/minikafka/log/segment.py"
    ```diff
    diff --git a/src/minikafka/log/segment.py b/src/minikafka/log/segment.py
    index e70b3546183be34cd87ade43f2e53cd5eb13ed4a..f05b248140dbd61b611ace58eab09792aa4fcd0f 100644
    --- a/src/minikafka/log/segment.py
    +++ b/src/minikafka/log/segment.py
    @@ -178,6 +178,15 @@ class Segment:
         def has_data(self) -> bool:
             return self.leo > self.base_offset

    +    @property
    +    def total_size_bytes(self) -> int:
    +        index_size = (
    +            self.index_path.stat().st_size
    +            if self.index_path.exists()
    +            else 0
    +        )
    +        return self.size_bytes + index_size
    +
         def append(self, batch: RecordBatch) -> BatchLocation:
             if batch.base_offset != self.leo:
                 raise InvalidRecord(
    ```

**What it is and why it appears**

Retention is physical deletion of old closed segments. Log start offset advances, while existing record offsets never change. The active segment is not a deletion candidate.

**Runtime role**

The manager evaluates age or total bytes from oldest to newest, stops at the first ineligible boundary, deletes selected files, and refreshes the partition view.

**Statement understanding**

Deleting a prefix preserves a single monotonic log-start boundary; deleting a middle segment would create an unexplained physical hole.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/10-retention/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Deleting a prefix preserves a single monotonic log-start boundary; deleting a middle segment would create an unexplained physical hole.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 3](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/03-retention-compaction.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/10-retention/stage.patch)
