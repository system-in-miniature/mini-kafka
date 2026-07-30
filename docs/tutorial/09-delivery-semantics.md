# Chapter 9 · Delivery Semantics Lab

“At most once,” “at least once,” and “exactly once” are not switches attached to
a queue. They are end-to-end statements about acknowledgement, retries,
processing side effects, offset publication, replication, and failure timing.
This lab chapter uses MiniKafka to make each boundary observable.

## Learning objectives

By the end of this chapter, you can:

- reproduce loss of an acknowledged `acks=1` write after failover;
- explain duplicate processing and skipped processing from commit ordering;
- trace PID, epoch, and sequence-based producer deduplication;
- enumerate the mechanisms composed by Kafka exactly-once processing; and
- scope every delivery claim to a failure model and an observable boundary.

## Acknowledgement is not commitment

`src/minikafka/replication/replica_set.py`,
`PartitionReplicaSet.append`, returns immediately after leader append for
`AckMode.LEADER` (`acks=1`). The follower may still have a smaller LEO, and HW
may still point before the new record. The producer has an offset, but ordinary
consumers cannot yet read the record.

If the leader fails in that interval,
`PartitionReplicaSet.promote` chooses an ISR candidate and truncates it to the
old HW. A leader-only tail is absent from the candidate and disappears from the
new authoritative history. `src/minikafka/labs/leader_failure.py`, `main`,
constructs exactly this schedule without nondeterministic sleeps.

This is the durability risk of Kafka `acks=1`: leader acknowledgement alone
does not prove replication. MiniKafka's manual promotion algorithm is not
Kafka's election protocol, so the lab transfers the risk, not the precise
failover implementation.

`acks=all` plus a suitable `min.insync.replicas` closes this specific window by
waiting for the captured ISR and rejecting an insufficient ISR. It does not
solve ambiguous retries on its own. A response can be lost after append, or
MiniKafka can raise `NotEnoughReplicasAfterAppend`. The producer still needs a
way to retry without creating another record.

## Idempotence converts a retry into the original result

MiniKafka models the broker-side core of Kafka's idempotent producer in
`src/minikafka/producer/state.py`, `ProducerStateManager`.

An idempotent batch carries:

- a producer ID (PID), identifying the producer lineage;
- a producer epoch, fencing older instances; and
- a base sequence, ordering batches within a partition.

`ProducerStateManager.validate` first rejects an epoch older than the registered
or recorded epoch. It then checks whether the incoming batch's sequence range
exactly matches the latest recorded batch. An exact match returns the previous
`ProduceResult`; no second append occurs. Otherwise the next base sequence must
equal `previous.last_sequence + 1`, or `OutOfOrderSequence` is raised.

`src/minikafka/replication/replica_set.py`,
`PartitionReplicaSet.append`, calls `validate` before touching the leader log
and calls `record` after append. On replica-set construction and after
promotion, `ProducerStateManager` rebuilds its state from batches already in
the leader log. Therefore restart does not automatically forget the last
accepted sequence.

The public construction path is `src/minikafka/core/cluster.py`,
`BrokerCluster.producer`. With `idempotent=True`, it forces `acks=all` when the
caller left the default `acks=1`, rejects an explicit incompatible ack mode,
allocates a PID/epoch through `ProducerIdentityStore.allocate`, and registers
the epoch with every replica set. Reusing `transactional_name` allocates the
same PID with a higher epoch, fencing the older producer object.

There are deliberate limits. State is keyed only by PID inside each partition's
manager, and the duplicate window stores one latest batch. Kafka keeps five
recent batches to match its allowed in-flight window. In MiniKafka a retry for
batch N after batch N+1 was accepted is an out-of-order error rather than a
deduplicated response. Identity allocation is a local JSON file, not broker
coordination.

## Consumer ordering chooses replay or loss

`src/minikafka/consumer/consumer.py`, `Consumer.poll`, advances a consumer
object's local position. `Consumer.commit` separately publishes that next
offset. No method can atomically combine an arbitrary external side effect with
the offset store.

Consider record offset 0:

1. poll advances position to 1;
2. application performs its side effect;
3. process crashes before committing 1.

A replacement begins at committed offset 0 and repeats the side effect. This is
the standard at-least-once shape: no acknowledged work is intentionally
skipped, but duplicates are possible.

Reverse steps 2 and 3:

1. poll advances position to 1;
2. commit 1;
3. process crashes before the side effect.

The replacement starts at 1 and never processes offset 0. This is the
at-most-once shape: duplicates are avoided by accepting possible loss.

The repository locks down both schedules in
`tests/consumer/test_delivery_semantics.py`:
`test_process_before_commit_can_replay_after_crash` and
`test_commit_before_process_can_skip_after_crash`. These are application
delivery semantics even though the broker log itself remains intact.

## Exactly once is a composition

For Kafka-style read-process-write, “exactly once” requires a chain of
mechanisms:

1. **Durable input:** consumers read a replicated committed prefix.
2. **Idempotent output production:** retries reuse PID/epoch/sequence instead of
   duplicating output.
3. **Zombie fencing:** a newer producer epoch rejects writes from an older
   instance with the same transactional identity.
4. **Atomic output and input offsets:** produced records and the next consumed
   offsets commit or abort as one transaction.
5. **Isolation:** downstream consumers use `read_committed`, respecting LSO and
   ignoring aborted batches.
6. **A scoped side-effect boundary:** the guarantee covers Kafka records and
   offsets in the transaction, not arbitrary email, HTTP, or database writes
   unless those systems join through their own idempotency or transaction
   protocol.

MiniKafka implements these pieces in two adjacent but not fully connected
tracks. `src/minikafka/producer/state.py` models idempotent PID/epoch/sequence
for ordinary producers. `src/minikafka/transaction/manager.py` models
transactional markers, LSO, and atomic staged offsets, with data and markers
using `acks=all`. The transaction send path does **not** carry producer
identity or epoch. Consequently this repository demonstrates the composition
requirements but does not claim Kafka-compatible exactly-once processing.

That negative result is useful: it prevents the common equation
“transactional API + successful demo = exactly once.” A guarantee exists only
when every failure edge between input, output, retry, recovery, and visibility
is closed.

## Compared with real Kafka

Consult the
[producer acknowledgement mapping](../kafka-mapping.md#producer-and-acknowledgement-path),
[transaction mapping](../kafka-mapping.md#transactions-and-visibility), and
[behavior matrix](../behavior-matrix.md).

The acknowledged-write-loss risk for `acks=1`, exact latest-batch retry
deduplication, epoch fencing, control markers, LSO, and committed-offset
publication all represent real Kafka ideas. The important reductions are:

- MiniKafka steps follower fetch and promotion explicitly; Kafka uses networked
  brokers and controller-managed elections.
- `acks=0` in the Direct API still performs the append before returning unknown
  offsets. Kafka sends no response, so transport timing is different.
- MiniKafka's `acks=all` waiter captures an ISR set and waits for every member;
  Kafka uses its full replication/HW machinery.
- MiniKafka retains one duplicate batch instead of Kafka's five.
- MiniKafka transactions are not built on its idempotent producer state and
  have no transactional-ID zombie fencing.
- Transaction and offset coordinators are local files/objects, not replicated
  Kafka internal topics.

When stating a delivery guarantee in design review, name the unit (“records in
one Kafka transaction”), the reader isolation, the acknowledgement and retry
policy, and excluded external side effects.

## Lab 1: lose an acknowledged leader-only write

Run:

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run python -m minikafka.labs.leader_failure
```

Measured output:

```text
1. Producer uses acks=1 (leader acknowledgement only).
   acknowledged offset: 0
   consumer-visible end (HW): 0
   The write is acknowledged but has not reached the follower.
2. Simulate the leader failing before follower replication.
   broker 2 is promoted; its log did not contain offset 0.
   records after failover: 0
3. Result: the acknowledged write was lost.
   Kafka has the same acks=1 risk; use acks=all with an appropriate min.insync.replicas for stronger durability.
```

The two zeroes mean different things. `acknowledged offset: 0` is the assigned
record offset. `consumer-visible end (HW): 0` is an exclusive boundary, so no
record is visible. After promotion, the result length confirms the tail is gone.

## Lab 2: prove exact retry deduplication

Run the focused producer test:

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run pytest -q \
  tests/producer/test_idempotence.py::test_exact_retry_returns_original_offsets
```

Measured output:

```text
.                                                                        [100%]
1 passed in 0.08s
```

The test creates one sequenced batch, waits for `acks=all`, then submits the
same batch again. It asserts that both `ProduceResult` values are equal and
leader LEO remains 1. Passing therefore proves “returned the original result
without a second append,” not merely “no exception.”

Run the consumer failure schedules as a third check:

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run pytest -q \
  tests/consumer/test_delivery_semantics.py
```

Measured output:

```text
..                                                                       [100%]
2 passed in 0.12s
```

## Exercises

1. **Understanding:** A producer gets a post-append error and retries the same
   payload with a new sequence number. Why can content equality not make this
   safely idempotent?

    ??? note "Reference answer"

        Two legitimate business events may have identical bytes. Deduplication
        needs request identity and ordering, not payload equality. The retry
        must reuse the same PID, epoch, and sequence so the broker can return
        the original result.

2. **Hands-on:** Copy the `sequenced` helper and exact-retry scenario from
   `tests/producer/test_idempotence.py` into `/tmp/dedup_lab.py`, print LEO after
   each send, then change the retry sequence from 0 to 1. Acceptance: the exact
   retry prints `1, 1`; sequence 1 is a new append and prints final LEO 2. Do
   not edit `src/`.

    ??? note "Reference answer"

        Sequence 0 exactly matches the latest recorded batch, so
        `validate` returns its saved result. Sequence 1 equals the expected
        next sequence and represents a new record, so it is appended. Reusing
        the same payload does not change either decision.

3. **Hands-on:** Write a scratch consumer test that polls one record, records a
   side effect in a Python list, does not commit, recreates the consumer, and
   polls again. Acceptance: the list contains the value twice and the durable
   committed offset remains `None`.

    ??? note "Reference answer"

        Use one group ID and directly assign the same partition in two consumer
        objects. Process the first poll without `commit`; close/recreate and
        poll from earliest. Appending both results to the list demonstrates
        replay. This is at-least-once processing, not broker duplication.

4. **Understanding:** Does MiniKafka currently provide end-to-end exactly once
   for its transaction API?

    ??? note "Reference answer"

        No. It provides markers, LSO/read-committed visibility, atomic staged
        offsets, and `acks=all` transaction appends. Its transaction path is
        separate from PID/epoch/sequence state, so it lacks idempotent
        transactional retries and transactional-ID zombie fencing. External
        side effects are also outside the boundary.

## Summary

Delivery semantics emerge from failure ordering. `acks=1` can lose an
acknowledged leader-only tail; process-before-commit can replay; commit-before-
process can skip; PID/epoch/sequence makes a precise retry idempotent; and
transactions plus read-committed isolation atomically publish Kafka-contained
work. MiniKafka exposes each ingredient and the missing connection between
idempotence and transactions. The final chapter now places a transport around
this semantic core and explains why that does not make it a Kafka broker.
