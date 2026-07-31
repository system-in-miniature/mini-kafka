# 快速开始

> [English quick start](../quickstart.md) · 中文快速开始

先安装 MiniKafka 并运行一个确定性的故障实验，再继续阅读架构与行为参考资料。

MiniKafka 是一个直接 API 优先（direct-first）的 Python 参考实现，用一个小型、
可执行的系统呈现 Kafka 最有辨识度的领域语义：分区追加日志、偏移量、消费组、
复制、高水位、幂等写入与事务。它用于学习这些机制，不兼容 Kafka 线协议。

## 安装

需要 Python 3.12+ 与 [uv](https://docs.astral.sh/uv/)。

```bash
git clone https://github.com/system-in-miniature/mini-kafka.git
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

完整 API、功能范围和验证命令见
[仓库中文 README](https://github.com/system-in-miniature/mini-kafka/blob/main/README.zh-CN.md)。

## 下一步

继续阅读[架构总览](architecture.md)，了解实验中的每一步分别落在代码库的
什么位置。
