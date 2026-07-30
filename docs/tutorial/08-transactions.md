# Chapter 8 · Transactions

Replication answers “which log prefix is durable enough to read?” Transactions
add another question: “which records belong to a completed unit of work?” A
transaction may append output to several partitions and stage consumed offsets.
Readers using `read_committed` must see neither an unfinished transaction nor
an aborted one, while a successful commit must publish output and input
progress together.

## Learning objectives

By the end of this chapter, you can:

- trace MiniKafka's transaction states and journal records;
- explain PREPARE as the durable decision boundary for recovery;
- distinguish HW from the last stable offset (LSO);
- show how commit and abort control markers affect `read_committed`; and
- explain why `acks=all` is necessary but not sufficient for Kafka transactions.

## A durable state machine before a visibility rule

`src/minikafka/transaction/model.py`, `TransactionState`, defines five states:
`ONGOING`, `PREPARE_COMMIT`, `COMPLETE_COMMIT`, `PREPARE_ABORT`, and
`COMPLETE_ABORT`. `TransactionData` associates a transaction ID with touched
partitions, the first offset written in each partition, and staged group
offsets.

Those fields answer different recovery questions. `partitions` tells commit or
abort where markers belong. `first_offsets` tells readers where an unstable
transaction begins. `staged_offsets` holds consumer progress that must not
become visible early.

`src/minikafka/transaction/journal.py`, `TransactionJournal.append`, serializes
the whole current state as one JSON payload, prefixes it with CRC32, appends a
newline, flushes, and calls `fsync`. The journal is a sequence of state
snapshots, not an in-place database. `TransactionJournal.recover` scans valid
CRC-framed lines and keeps the last state for each transaction ID. If the final
line is incomplete, malformed, or has a bad CRC, it truncates the file to the
last valid boundary.

This is the same tail-repair idea used by append-only logs: a crash can leave an
incomplete final write, but it must not turn arbitrary earlier corruption into
valid state. The journal is local and single-process; it is not replicated by
MiniKafka's partition replication. A deliberate limitation is that recovery
stops at the first bad line and truncates **all content after it**, even if
later lines are independently well-formed; this is tail repair, not
resynchronization past middle corruption.

`src/minikafka/transaction/manager.py`, `TransactionManager.__init__`, recovers
the journal and immediately calls `TransactionManager._finish_recovery`.
PREPARE states matter because they record a durable decision:

- `PREPARE_COMMIT` publishes staged offsets synchronously and becomes
  `COMPLETE_COMMIT`.
- `PREPARE_ABORT` clears staged offsets and becomes `COMPLETE_ABORT`.

The recovery method does not append missing control markers. Normal commit
writes PREPARE before markers, so a crash in that interval can yield a
transaction the journal considers committed even if not every marker reached
the logs. This is a deliberate simplification and an important boundary when
comparing with Kafka's coordinator protocol.

## Transactional data is appended but initially unstable

`TransactionManager.begin` rejects reuse of an active ID, creates an
`ONGOING` record, and journals it. `Transaction.send` delegates to
`TransactionManager.send`, which first requires `ONGOING`, chooses a partition,
and builds a `RecordBatch` carrying `transactional_id`.

The batch is appended through `BrokerCluster.append_batch` with
`AckMode.ALL`. After success, the manager remembers the partition and its first
base offset, then journals the new state. Using `acks=all` ensures the method
does not report the transactional data step complete while a captured ISR
replica is still missing it.

This path does not use producer PID, epoch, or sequence. The transaction ID is
only a journal/visibility identity. That difference becomes crucial later:
durable replication is not the same as idempotent retry or zombie-producer
fencing.

`Transaction.send_offsets` does not write `OffsetStore`. It updates
`staged_offsets` and journals the state. Until commit, a new consumer still
reads the old committed offset.

## PREPARE, markers, and completion

`src/minikafka/transaction/manager.py`, `TransactionManager.commit`, performs
these ordered steps:

1. require `ONGOING`;
2. set `PREPARE_COMMIT` and journal it;
3. append a COMMIT control marker with `acks=all` to every touched partition;
4. publish all staged offsets through `OffsetStore.commit_many`; and
5. set and journal `COMPLETE_COMMIT`.

`TransactionManager.abort` mirrors the flow with `PREPARE_ABORT`, ABORT
markers, clearing staged offsets, and `COMPLETE_ABORT`.

Control batches come from `src/minikafka/core/batch.py`,
`RecordBatch.control_marker`. They contain no user records, require a
transaction ID, consume one logical offset, and carry either `ControlType.COMMIT`
or `ControlType.ABORT`. Consumers do not return these control records, but their
location closes the transaction in the log.

Both data and markers use `acks=all`. The focused tests in
`tests/transaction/test_visibility.py` and `tests/transaction/test_abort.py`
start each operation as a task on a two-replica partition and show that it
remains pending until `BrokerCluster.replicate_all_once` advances followers.
This avoids a dangerous half-contract in which transaction data is replicated
but the decision marker is only leader-local.

Atomic output-plus-offset publication here is coordinator-local. The code
writes all output markers before it publishes staged offsets. If an abort
occurs, staged offsets are discarded. It does not provide a distributed atomic
commit across independent coordinators or clusters.

## HW and LSO answer different questions

HW is the exclusive end replicated according to the current ISR boundary. It
may include data from an open transaction. `read_uncommitted` reads up to HW,
so it can observe that data.

`src/minikafka/transaction/manager.py`,
`TransactionManager.last_stable_offset`, finds the smallest first offset among
transactions in `ONGOING`, `PREPARE_COMMIT`, or `PREPARE_ABORT`, defaulting to
HW when none are unstable. This value is the LSO. It prevents a
`read_committed` consumer from skipping across an unfinished transaction to
later records, because the outcome of the earlier transaction is not settled.

`src/minikafka/core/cluster.py`, `BrokerCluster.fetch`, applies LSO as
`end_offset` for `IsolationLevel.READ_COMMITTED`. It then skips control batches
and skips transactional batches unless
`TransactionManager.is_committed` reports `COMPLETE_COMMIT`. Aborted records
remain physically in the log but are never returned at that isolation level.

Two filters are needed:

- LSO blocks all records at and after the first unstable transaction.
- committed-state filtering hides aborted transactional batches below the
  now-advanced LSO.

`src/minikafka/replication/replica_set.py`,
`PartitionReplicaSet.fetch`, contains the same conceptual LSO and state filters
for its async white-box fetch path. The public cluster fetch remains the normal
Direct API.

## Compared with real Kafka

Use the repository's
[transactions and visibility mapping](../kafka-mapping.md#transactions-and-visibility)
and [behavior matrix](../behavior-matrix.md) as the transfer boundary.

MiniKafka preserves several central ideas from KIP-98: transactional record
batches, commit/abort control markers, LSO-bounded `read_committed`, and staged
input offsets that publish only on commit. Data and markers are replicated with
`acks=all`.

The missing machinery is just as important:

- Kafka transaction coordinators persist state in the replicated
  `__transaction_state` internal topic. MiniKafka has one local CRC journal.
- Kafka transactions build on idempotent producers. A `transactional.id`
  identifies producer epochs so a newer instance fences a zombie. MiniKafka's
  transaction ID is independent of `ProducerStateManager`; transactional sends
  have no PID/epoch/sequence.
- Kafka coordinates marker completion, retries, timeouts, and coordinator
  migration. MiniKafka completes PREPARE locally on startup and does not repair
  missing markers during that recovery.
- Kafka's `sendOffsetsToTransaction` ties offsets to group metadata and
  coordinator protocols. MiniKafka stages a map and publishes it to a local
  JSON `OffsetStore`.

Therefore “MiniKafka uses `acks=all`” proves a replication property, not
exactly-once processing by itself. Exactly-once also needs idempotent production,
zombie fencing, read-committed consumption, and atomic offset publication.

Recovery should be evaluated at every cut between the five commit steps, not
only before and after the whole method. Ask which decision is durable, which
markers exist, whether offsets have published, and what a restarted coordinator
will do. That cut-point analysis reveals why PREPARE is necessary and also why
MiniKafka's local completion rule is narrower than Kafka's distributed
transaction recovery.

## Hands-on experiment: see the visibility boundary

Run this socket-free, temporary-directory experiment from the repository root:

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run python - <<'PY'
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition
from minikafka.clock import ManualClock
from minikafka.replication.model import IsolationLevel

async def main():
    with TemporaryDirectory() as d:
        async with BrokerCluster.open(
            MiniKafkaConfig(data_dir=Path(d)), clock=ManualClock()
        ) as cluster:
            await cluster.create_topic("out", 1, 1)
            tp = TopicPartition("out", 0)
            tx = await cluster.transactions.begin("tx-1")
            await tx.send("out", value=b"pending")
            print("before marker, read_uncommitted:",
                  [r.value for r in cluster.fetch(tp, 0, 10)])
            print("before marker, read_committed:",
                  [r.value for r in cluster.fetch(
                      tp, 0, 10, IsolationLevel.READ_COMMITTED)])
            await tx.commit()
            print("after commit marker:",
                  [r.value for r in cluster.fetch(
                      tp, 0, 10, IsolationLevel.READ_COMMITTED)])
            tx2 = await cluster.transactions.begin("tx-2")
            await tx2.send("out", value=b"discard-me")
            await tx2.abort()
            print("after abort marker:",
                  [r.value for r in cluster.fetch(
                      tp, 0, 10, IsolationLevel.READ_COMMITTED)])

asyncio.run(main())
PY
```

Measured output:

```text
before marker, read_uncommitted: [b'pending']
before marker, read_committed: []
after commit marker: [b'pending']
after abort marker: [b'pending']
```

The aborted value did not vanish from disk; isolation filtered it. Inspecting
`cluster.leader_log(tp).all_batches()` would reveal data and both control
markers, while the consumer-facing result remains one committed value.

For the two-replica `acks=all` part of the contract, run:

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run pytest -q \
  tests/transaction/test_visibility.py tests/transaction/test_abort.py
```

Measured output:

```text
......                                                                   [100%]
6 passed in 0.48s
```

## Exercises

1. **Understanding:** Why can `read_committed` not simply skip the open
   transaction and return later non-transactional records?

    ??? note "Reference answer"

        Doing so would expose records beyond an unresolved ordering boundary.
        The open transaction may later commit, and returning later offsets
        first would violate a stable prefix view. LSO stops at the first
        unstable transaction; after its outcome is known, scanning can proceed
        and filter aborted batches.

2. **Hands-on:** Extend the experiment in `/tmp/tx_offsets.py` with an input
   `TopicPartition` and `await tx.send_offsets("workers", {input_tp: 7})`.
   Print the committed offset before and after commit. Acceptance: it is
   `None` before commit and `7` after. Do not edit `src/`.

    ??? note "Reference answer"

        Query `await cluster.offsets.get("workers", input_tp)` on both sides of
        `await tx.commit()`. `send_offsets` changes only journaled
        `staged_offsets`; `TransactionManager.commit` publishes the map after
        all commit markers succeed.

3. **Hands-on recovery:** Run only
   `tests/reliability/test_transaction_restart.py`, then explain the three
   cases it proves. Acceptance: `3 passed` and an explanation mentioning
   committed visibility, PREPARE_COMMIT offset completion, and bad-tail
   truncation.

    ??? note "Reference answer"

        Use `uv run pytest -q
        tests/reliability/test_transaction_restart.py`. The tests show that a
        completed transaction remains visible after reopen, recovery turns a
        journaled PREPARE_COMMIT into committed offsets plus COMPLETE_COMMIT,
        and an incomplete final journal frame is truncated. They do not prove
        replicated coordinator failover.

4. **Understanding:** What additional identity would be needed to fence two
   processes using the same transaction name?

    ??? note "Reference answer"

        A coordinator-issued producer ID and monotonically increasing producer
        epoch bound to the transactional ID. Every transactional write must
        carry that epoch so a newer initialization fences the old process.
        MiniKafka's transaction path currently does not connect to that state.

## Summary

MiniKafka transactions layer a journaled decision state and an LSO visibility
boundary on top of replicated logs. PREPARE records the recovery decision,
markers settle partition visibility, and staged offsets publish only after a
commit. This teaches the composition, while the local journal and missing
transactional producer fencing mark the boundary from real Kafka. The next
chapter turns these mechanisms into failure experiments and assembles the
ingredients behind at-most-once, at-least-once, and exactly-once claims.
