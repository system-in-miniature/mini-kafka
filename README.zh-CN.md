> **语言**: [English](README.md) | 简体中文

# MiniKafka

[![CI](https://github.com/system-in-miniature/mini-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/system-in-miniature/mini-kafka/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) ![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-blue.svg)

MiniKafka 是 Kafka 独特领域语义的一套直接 API 优先（direct-first）参考实现。
它是一个分区式、可重放的事件日志，而非二进制协议练习，也不兼容 Kafka 线协议。

本项目聚焦于那些让 Kafka 成为 Kafka 的机制：

- 仅追加分区日志（append-only partition log）、偏移量（offset）、日志段（segment）、
  稀疏索引（sparse index）与 CRC 恢复；
- 按键分区（keyed partitioning）、批处理（batching）、消费者位置
  （consumer position）、重放（replay）与滞后（lag）；
- 消费者组所有权（consumer-group ownership）、再均衡代次
  （rebalance generation）与过期成员隔离（fencing）；
- 保留策略（retention）、按键压缩（keyed compaction）、墓碑（tombstone）与
  保留的偏移量空洞；
- 领导者/跟随者（leader/follower）拉取式复制、同步副本集合
  （in-sync replicas, ISR）、高水位（high watermark, HW）与确认模式
  （ack mode）；
- 领导者任期（leader epoch）、安全提升与分歧尾部截断；
- 生产者序列去重（deduplication）与任期隔离；
- 事务标记（transaction marker）、读取隔离（read isolation）与输出加偏移量的
  原子提交。

TCP 被有意设计为一个薄适配器。自动选举、KRaft 和通用故障检测被有意排除在
数据平面核心之外。

## 学习模式

- **机制教程**——通过[双语十章教程](docs/zh/tutorial/index.md)学习概念与运行路径。
- **自主重建**——通过二十个可独立浏览的 Stage 重建 MiniKafka，每节包含失败证据与
  按机制分组的 Diff，入口见[重建旅程](docs/zh/journey/index.md)。
- **Agent 带教**——让 Codex 在终端中互动讲解、实现并验收一个 Stage，使用方式见
  [CLI 教程](docs/zh/agent-guide.md)。

## 直接 API

```python
import asyncio
from pathlib import Path

from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition


async def main() -> None:
    config = MiniKafkaConfig(data_dir=Path("./data"))
    async with BrokerCluster.open(config) as cluster:
        await cluster.create_topic("orders", partitions=3, replication_factor=1)
        producer = cluster.producer(batch_size=1)
        metadata = await producer.send(
            "orders", key=b"user-42", value=b"created"
        )
        records = cluster.fetch(
            TopicPartition("orders", metadata.partition), 0, 100
        )
        print(records)


asyncio.run(main())
```

核心测试直接调用 `BrokerCluster`、`PartitionLog`、`GroupCoordinator` 和
`PartitionReplicaSet`。证明领域正确性不需要套接字。

## 可选 JSON/TCP 适配器

`JsonTcpServer` 接受有界、以换行符分隔的 JSON。二进制键和值使用显式的
base64 字段。它支持主题管理、元数据、生产/拉取、消费者组加入/心跳以及
偏移量提交。它用于证明传输层分离，而非模拟 Kafka 持续演进的线协议。

## 可靠性实验

测试套件以确定性方式展示：

- `acks=1` 可以确认仅存在于领导者尾部、且在提升后丢失的数据；
- `acks=all` 会等待追加时捕获的 ISR，并遵守最小 ISR；
- 消费者无法看到高水位以上的记录；
- 过期领导者和过期消费者组代次会被隔离；
- 旧领导者重新加入前会截断分歧的未提交尾部；
- 重试会按生产者任期和序列号去重；
- 已中止和未完成的事务对 `read_committed` 保持不可见；
- 崩溃恢复会修复不完整批次、索引和日志尾部。

两个面向读者的实验（labs）将最重要的权衡转化为带讲解的实验：

```bash
uv run python -m minikafka.labs.leader_failure
uv run python -m minikafka.labs.rebalance
```

运行：

```bash
uv sync
uv run pytest -q
uv run ruff check .
uv run python -m compileall -q src tests
uv run python tools/count_sloc.py
```

## 范围与简化

这有意不是一个生产级代理。它使用自定义记录格式、手动领导者提升、单进程代理
模拟和有界事务协调器。它不实现 Kafka 线协议兼容性、KRaft/ZooKeeper、
TLS/SASL/ACL、配额、机架感知放置、分层存储、Kafka Connect、Kafka Streams
或生产级事务协议。

重要的语义差异均被明确列出：

- 幂等生产者去重会为每个生产者/分区记住一个批次，而 Kafka 保留最近五个；
  更旧的乱序重试会被拒绝，而不是被判定为重复；
- 启动时根据副本日志末端偏移量（log end offset, LEO）重建高水位，而不是从
  Kafka 风格的复制检查点文件加载；
- 故障转移在高水位处截断，而不是使用 KIP-101 的领导者任期分歧协议，并且
  MiniKafka 可能截断刚被提升的副本本身——该行为在语义上与 Kafka 相反；
- `PartitionLog.truncate_to` 会将保留日志重写到一个新日志段中，启动时则扫描
  每个批次以恢复最大领导者任期；Kafka 会截断尾部文件并使用领导者任期检查点；
- 记录批次始终不压缩。没有 gzip 或其他压缩实现；
- 消费者组再均衡是协调器侧立即进行的分配，而不是 Kafka 的两阶段
  JoinGroup/SyncGroup 协议，也没有撤销屏障或协作式再均衡；
- 已提交偏移量存放在以原子方式替换的 JSON 文件中，而不是 Kafka 的压缩主题
  `__consumer_offsets`；
- 事务现在对数据和标记使用 `acks=all`，但并非构建于幂等生产者的 PID/任期
  序列状态之上，因此这不是 Kafka 隔离僵尸生产者的事务协议；
- 保留策略只删除连续的已关闭日志段前缀，与 Kafka 不产生中间空洞的不变量一致；
  时间戳索引和 `offsetsForTimes` 仍未实现；
- 压缩会重建一个同级目录，并通过两次原子重命名完成安装。这两次操作的组合并非
  一次原子交换，尽管进程内故障会回滚；这是教学实现，而不是 Kafka 后台日志
  清理器的文件生命周期。

完整的分级比较及相关 Kafka 配置与 KIP 参考，请参阅
[MiniKafka → Kafka 映射](docs/zh/kafka-mapping.md)。

本实现遵循 Apache Kafka 官方文档中记录的
[日志实现](https://kafka.apache.org/43/implementation/log/)、
[消息格式](https://kafka.apache.org/43/implementation/message-format/)、
[生产者配置](https://kafka.apache.org/43/configuration/producer-configs/)
和[代理配置](https://kafka.apache.org/43/configuration/broker-configs/)概念。

## 仓库边界

参考实现、机制教程与重建旅程放在同一仓库中，让每条学习结论都能指向可执行证据。
Journey 只拥有教学产物；最终 Stage 必须与参考源码及行为测试逐字节一致。

## 商标声明

MiniKafka 是独立的教学项目，与 the Apache Software Foundation 无隶属、背书或赞助关系。"Apache Kafka" 商标归其所有者所有。
