# Hands-on Labs

> English · [中文版](zh/labs-guide.md)

Install the project once from the repository root:

```bash
uv sync
```

Both labs use temporary directories and the public Direct API, so they leave no
broker data behind.

## 1. Leader failure with `acks=1`

```bash
uv run python -m minikafka.labs.leader_failure
```

Expected landmarks:

```text
acknowledged offset: 0
consumer-visible end (HW): 0
records after failover: 0
```

Watch the distinction between producer acknowledgement and committed
visibility. The leader accepts offset `0`, but the follower has not fetched it.
Promoting that follower removes the unreplicated tail. This is the same
durability risk represented by Kafka's `acks=1`, not a claim that MiniKafka
implements Kafka's election protocol.

## 2. Consumer-group rebalance

```bash
uv run python -m minikafka.labs.rebalance
```

Expected landmarks:

```text
member 1: orders-0, orders-1, orders-2, orders-3
member 2: orders-1, orders-3
overlap after refresh: False
```

Watch the generation transition. The first consumer initially owns four
partitions; after the second joins, its old local assignment remains until
`refresh_assignment()`. The refreshed assignments no longer overlap. When the
second member leaves, the first owns all partitions again. MiniKafka performs
this reassignment synchronously and has no Kafka-style revoke barrier.

## Continue with executable evidence

The labs narrate two trade-offs; the test suite covers the full mechanism set.
Use the [behavior matrix](behavior-matrix.md) to choose a focused test, or run:

```bash
uv run pytest -q
```
