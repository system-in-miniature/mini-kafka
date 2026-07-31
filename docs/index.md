# MiniKafka Tutorial / MiniKafka 教程

> English quick start / 英文快速开始 · [Chinese edition / 中文版](zh/index.md)

MiniKafka is a direct-first Python reference implementation of Kafka's most
distinctive domain semantics: partitioned append-only logs, offsets, consumer
groups, replication, high watermarks, idempotence, and transactions. It is a
small executable system for studying those mechanisms, not a Kafka
wire-compatible broker.

MiniKafka 是一个直接 API 优先（direct-first）的 Python 参考实现，用一个小型、
可执行的系统呈现 Kafka 最有辨识度的领域语义：分区追加日志、偏移量、消费组、
复制、高水位、幂等写入与事务。它用于学习这些机制，不兼容 Kafka 线协议。

## Install / 安装

You need Python 3.12+ and [uv](https://docs.astral.sh/uv/).

需要 Python 3.12+ 与 [uv](https://docs.astral.sh/uv/)。

```bash
git clone https://github.com/system-in-miniature/mini-kafka.git
cd MiniKafka
uv sync
```

## First experiment / 第一个实验

Run the acknowledged-write-loss lab:

运行“已确认写入丢失”实验：

```bash
uv run python -m minikafka.labs.leader_failure
```

The producer first receives an acknowledgement for offset `0` with `acks=1`.
After the unreplicated leader is replaced, the lab prints
`records after failover: 0`. This is the durability trade-off behind Kafka's
acknowledgement settings, made deterministic in a two-broker simulation.

生产者先以 `acks=1` 收到偏移量 `0` 的确认；尚未复制的领导者被替换后，输出中
会出现 `records after failover: 0`。这把 Kafka 确认级别的持久性权衡压缩成了
一个确定性的双代理模拟。

Continue with the [architecture tour](architecture.md), then compare each
mechanism with [Apache Kafka](kafka-mapping.md).

接着阅读[架构总览](architecture.md)，再把机制逐项映射到
[Apache Kafka](kafka-mapping.md)。

For the full API, feature list, scope, and verification commands, read the
[repository README](https://github.com/system-in-miniature/mini-kafka/blob/main/README.md).

完整 API、功能范围和验证命令见
[仓库中文 README](https://github.com/system-in-miniature/mini-kafka/blob/main/README.zh-CN.md)。
