# Quick Start

> English · [中文快速开始](zh/index.md)

Install MiniKafka and run one deterministic failure experiment before reading
the architecture and behavior references.

MiniKafka is a direct-first Python reference implementation of Kafka's most
distinctive domain semantics: partitioned append-only logs, offsets, consumer
groups, replication, high watermarks, idempotence, and transactions. It is a
small executable system for studying those mechanisms, not a Kafka
wire-compatible broker.

## Install

You need Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/system-in-miniature/mini-kafka.git
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

For the full API, feature list, scope, and verification commands, read the
[repository README](https://github.com/system-in-miniature/mini-kafka/blob/main/README.md).

## Next step

Continue with the [Architecture Tour](architecture.md) to see where each part
of the experiment lives in the codebase.
