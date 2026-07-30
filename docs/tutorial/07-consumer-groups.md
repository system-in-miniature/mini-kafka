# Chapter 7 · Consumer Groups

A partition gives records a total order, but an application usually wants more
than one worker. A consumer group turns a set of independent consumers into one
logical subscriber: during a stable generation, each subscribed partition has
at most one owner in the group. MiniKafka keeps that central invariant while
making the coordinator small enough to read in one sitting.

## Learning objectives

By the end of this chapter, you can:

- trace a group through join, rebalance, stable operation, expiry, and leave;
- calculate MiniKafka's deterministic round-robin assignment;
- explain how generation and ownership checks fence stale offset commits;
- distinguish current position from a committed next offset; and
- state precisely how this coordinator simplifies Kafka rebalance protocols.

## The coordinator owns membership truth

The main state model is in `src/minikafka/consumer/group.py`. `Group` contains a
generation number, a `GroupState`, a member table, and the authoritative
assignment. Each `GroupMember` stores subscriptions and a session deadline.
The consumer object has a local copy, but the coordinator is the authority.

`GroupCoordinator.join` validates IDs and topic names, creates or replaces the
member, gives it a deadline of `now + session_timeout_ms`, and calls
`GroupCoordinator._rebalance`. That method makes the transition explicit:

1. set `PREPARING_REBALANCE`;
2. increment the generation;
3. compute assignment, or become `EMPTY`; and
4. set `STABLE`.

This sequence is synchronous. No other coroutine observes a long-lived
preparing phase in the normal implementation. The state exists to teach the
coordinator contract and to let `assignment` and `commit` reject work unless
the group is stable.

Joining again with the same member ID replaces that member's subscriptions and
also creates a new generation. `GroupCoordinator.leave` removes a known member
and uses the same rebalance path. A leave for an unknown member is idempotent.
These choices are visible in `src/minikafka/consumer/group.py`,
`GroupCoordinator.join` and `GroupCoordinator.leave`.

## Assignment is deterministic and subscription-aware

`src/minikafka/consumer/assignor.py`, `round_robin_assign`, first sorts member
IDs, topic names, and partition numbers. For each topic it builds the sorted
list of members subscribed to that topic, then advances one shared cursor over
eligible members. With members `a` and `b` subscribed to four partitions, the
result is `a -> (0, 2)` and `b -> (1, 3)`.

Sorting is more than cosmetic in a teaching system. It removes dictionary and
arrival-order accidents from tests. Given the same member/subscription map and
topic metadata, the assignor produces the same result.

The cursor is shared across topics rather than restarted for each topic. That
keeps distribution roughly round-robin across the full ordered partition
stream, but this is only one strategy. MiniKafka has no range, sticky, or
cooperative-sticky assignor and exposes no pluggable assignor negotiation.

`src/minikafka/consumer/consumer.py`, `Consumer.subscribe`, calls `join` and
copies the returned generation and assignment through `Consumer._apply_join`.
That helper also drops positions for partitions no longer owned. A later group
change does **not** push into old consumer objects. They must call
`Consumer.refresh_assignment` to read the new coordinator view.

That gap is deliberate and educational. Immediately after a second member
joins, the first object's local assignment may overlap the second's new
assignment. Local belief is stale; coordinator authority is not. A data-plane
caller should refresh, while the coordinator's commit fencing prevents the
stale owner from publishing progress.

## Generation fencing protects committed progress

A generation is an authority token for one complete assignment. Every
membership-changing rebalance increments it. In
`src/minikafka/consumer/group.py`,
`GroupCoordinator._validate_generation`, any mismatch raises
`IllegalGeneration`.

The check appears in two important paths:

- `GroupCoordinator.heartbeat` validates generation and membership before
  extending the deadline.
- `GroupCoordinator.commit` requires `STABLE`, validates generation and member
  existence, then checks that every partition in the request belongs to that
  member. A violation raises `NotPartitionOwner`.

Only after all checks pass does `GroupCoordinator.commit` call
`OffsetStore.commit`. Therefore an old worker cannot overwrite the new owner's
progress even during the local-assignment overlap described above.

`src/minikafka/consumer/consumer.py`, `Consumer.poll`, maintains a local
**position**: the next offset that this consumer object will fetch. Polling
advances it. `Consumer.commit` publishes those positions as the group's
committed next offsets. The two states are intentionally independent. A crash
after processing but before commit replays records; a commit before processing
can skip them.

For directly assigned consumers, `Consumer.commit` writes the offset store
without a coordinator generation because there is no subscription-based group
ownership to fence. For subscribed consumers it always routes through
`GroupCoordinator.commit`.

`src/minikafka/consumer/offsets.py`, `OffsetStore.commit_many_sync`, persists
the complete offset map using a temporary file, `fsync`, `os.replace`, and a
parent-directory `fsync`. The durable object is a local JSON file; it is not a
MiniKafka topic.

## Heartbeats are leases, not work acknowledgements

`GroupCoordinator.heartbeat` sets a new deadline based on the injected clock.
`GroupCoordinator.expire_members` scans groups, removes members for which
`now > deadline_ms`, and rebalances only groups that lost members. Notice the
strict `>`: at the exact deadline the member has not expired.

The public step is `src/minikafka/core/cluster.py`,
`BrokerCluster.expire_group_members`. Nothing runs automatically. Tests use
`src/minikafka/clock.py`, `ManualClock`, to advance time and call expiry
deterministically.

A heartbeat says “this member's session is alive.” It does not commit records,
prove that processing succeeded, or refresh another consumer object's local
assignment. Keeping liveness, ownership, local position, and committed progress
separate is the key mental model.

## Compared with real Kafka

The exact boundaries are listed in the
[consumer and group coordination mapping](../kafka-mapping.md#consumer-and-group-coordination);
the executable claims are indexed by the
[behavior matrix](../behavior-matrix.md).

MiniKafka preserves stable-generation single ownership, generation checks on
heartbeats and commits, and ownership checks on commits. Those are useful Kafka
invariants. The orchestration around them is intentionally reduced:

- Kafka's classic protocol separates JoinGroup and SyncGroup and selects a
  group leader to participate in assignment. MiniKafka joins and assigns
  synchronously on the coordinator.
- Kafka has rebalance timeouts and revoke/assignment callbacks. MiniKafka has
  no revoke barrier; local assignment changes only on explicit refresh.
- Kafka supports eager and cooperative protocols, including KIP-429.
  MiniKafka supplies one eager round-robin result.
- Real clients heartbeat in background and coordinate polling constraints.
  MiniKafka uses an injected clock and explicit expiry calls; it does not model
  KIP-62's background liveness split.
- Kafka stores group metadata and offsets in the replicated, compacted
  `__consumer_offsets` topic. MiniKafka uses one atomic JSON file, so its
  offset durability does not exercise the project's own replication or
  compaction.
- Error taxonomy is collapsed. An unknown group currently becomes
  `UnknownMember`, whereas Kafka distinguishes group and member errors.

Do not describe the MiniKafka coordinator as wire-compatible or production
scalable. Describe it as a deterministic model of ownership generations and
commit fencing.

Member identity deserves the same care as generation identity. MiniKafka
allocates process-local names in `src/minikafka/core/cluster.py`,
`BrokerCluster.consumer`, while Kafka clients may use generated or static
membership identities. A stable member identity can reduce unnecessary
movement in a production group, but it never replaces generation validation:
the coordinator must still prove that the member participates in the current
assignment. In other words, “who are you?” and “which assignment version are
you acting under?” are separate questions.

## Hands-on experiment: watch a rebalance

The repository includes a socket-free lab using temporary storage:

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run python -m minikafka.labs.rebalance
```

Measured output:

```text
1. First member joins and initially owns every partition.
   member 1: orders-0, orders-1, orders-2, orders-3
2. Second member joins, triggering a new generation.
   member 2: orders-1, orders-3
   member 1 local view before refresh: orders-0, orders-1, orders-2, orders-3
   MiniKafka deliberately has no revoke barrier: the old local assignment remains until refresh_assignment().
3. Member 1 refreshes and observes the stable assignment.
   member 1: orders-0, orders-2
   member 2: orders-1, orders-3
   overlap after refresh: False
4. Member 2 leaves; the remaining member owns all partitions.
   member 1: orders-0, orders-1, orders-2, orders-3
   Real Kafka coordinates JoinGroup/SyncGroup and revocation; this mini coordinator completes rebalance synchronously.
```

The interesting moment is between steps 2 and 3. Member 1's Python object still
lists all four partitions, but the coordinator generation already assigns two
of them to member 2. A stale commit from member 1 would fail before reaching
the offset store.

You can verify both fencing branches directly:

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run pytest -q \
  tests/consumer/test_generation_fencing.py
```

Measured output:

```text
..                                                                       [100%]
2 passed in 0.04s
```

The first test rejects an old generation after rebalance. The second rejects a
current-generation member attempting to commit a partition owned by someone
else.

## Exercises

1. **Understanding:** A consumer has polled through offset 9, so its position
   is 10, but its committed offset is 6. From which offset does a replacement
   consumer begin, and why?

    ??? note "Reference answer"

        It begins at committed offset 6. Position belongs to the live consumer
        object; the committed next offset is durable group progress. Offsets
        6 through 9 can replay, producing at-least-once behavior.

2. **Hands-on:** Copy the rebalance lab to `/tmp/rebalance_lab.py`. Before
   `first.refresh_assignment()`, call `await first.commit()` after initializing
   positions with a poll. Acceptance: catch and print
   `ILLEGAL_GENERATION`; after refresh, commit succeeds. Do not edit `src/`.

    ??? note "Reference answer"

        The first object retained the generation returned by its original
        join. The second join incremented the coordinator generation, so
        `GroupCoordinator._validate_generation` rejects the old token. Refresh
        copies the current generation and assignment, making a scoped commit
        valid.

3. **Hands-on:** Write a `/tmp` script with `ManualClock`, a 100 ms session
   timeout, and two members. Advance to exactly 100 ms, expire, then advance one
   more millisecond and expire again. Acceptance: the first result is empty and
   the second lists both expired members.

    ??? note "Reference answer"

        Join at clock zero gives each member deadline 100. The code tests
        `now > deadline_ms`, not `>=`, so expiry at 100 returns `()`. At 101,
        both are removed and one rebalance assigns the group as empty.

4. **Understanding:** Why is an ownership check still necessary after a valid
   generation check?

    ??? note "Reference answer"

        A generation identifies the assignment version, not a particular
        member's partitions. Two members share the same current generation.
        Without the ownership subset check, either could commit progress for
        the other's partitions.

## Summary

Consumer-group correctness rests on four separate states: coordinator
membership, generation-scoped assignment, a consumer's local fetch position,
and durable committed progress. MiniKafka collapses Kafka's rebalance
orchestration but retains the fencing checks that stop stale owners from
publishing offsets. The next chapter combines those offsets with output records
inside a transaction, adding a second visibility boundary beyond HW.
