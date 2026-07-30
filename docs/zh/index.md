# MiniKafka 教程

> [English](../index.md) · 中文快速开始

MiniKafka 是一个直接 API 优先（direct-first）的 Python 参考实现，用一个小型、
可执行的系统呈现 Kafka 最有辨识度的领域语义：分区追加日志、偏移量、消费组、
复制、高水位、幂等写入与事务。它用于学习这些机制，不兼容 Kafka 线协议。

English summary: MiniKafka makes Kafka's core data-plane mechanisms observable
in a small executable model; it is not a wire-compatible broker.

## 安装

需要 Python 3.12+ 与 [uv](https://docs.astral.sh/uv/)。

```bash
git clone https://github.com/system-in-miniature/MiniKafka.git
cd MiniKafka
uv sync
```

## 第一个实验

运行“已确认写入丢失”实验：

```bash
uv run python -m minikafka.labs.leader_failure
```

生产者先以 `acks=1` 收到偏移量 `0` 的确认；尚未复制的领导者被替换后，输出中
会出现 `records after failover: 0`。这把 Kafka 确认级别的持久性权衡压缩成了
一个确定性的双代理模拟。

接着阅读[架构总览](architecture.md)，再把机制逐项映射到
[Apache Kafka](kafka-mapping.md)。

完整 API、功能范围和验证命令见
[仓库中文 README](https://github.com/system-in-miniature/MiniKafka/blob/main/README.zh-CN.md)。
