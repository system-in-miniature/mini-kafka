# MiniKafka Tutorial

> English quick start · [中文版](zh/index.md)

MiniKafka is a direct-first Python reference implementation of Kafka's most
distinctive domain semantics: partitioned append-only logs, offsets, consumer
groups, replication, high watermarks, idempotence, and transactions. It is a
small executable system for studying those mechanisms, not a Kafka
wire-compatible broker.

中文简述：MiniKafka 用一个可运行的单进程参考实现展示 Kafka 的核心数据平面
机制；它适合观察日志、复制、消费组与事务语义，但不兼容 Kafka 线协议。

## Install

You need Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/system-in-miniature/MiniKafka.git
cd MiniKafka
uv sync
```

## First experiment

Run the acknowledged-write-loss lab:

```bash
uv run python -m minikafka.labs.leader_failure
```

The producer first receives an acknowledgement for offset `0` with `acks=1`.
After the unreplicated leader is replaced, the lab prints
`records after failover: 0`. This is the durability trade-off behind Kafka's
acknowledgement settings, made deterministic in a two-broker simulation.

Continue with the [architecture tour](architecture.md), then compare each
mechanism with [Apache Kafka](kafka-mapping.md).

For the full API, feature list, scope, and verification commands, read the
[repository README](https://github.com/system-in-miniature/MiniKafka/blob/main/README.md).
