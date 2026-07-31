# MiniKafka

MiniKafka is a compact, executable Python model for studying Kafka's core
data-plane mechanisms. It favors a direct API and deterministic experiments
over wire compatibility.

MiniKafka 是一个紧凑、可运行的 Python 教学模型，用于理解 Kafka 的核心数据平面
机制。它优先采用直接 API 与确定性实验，不追求线协议兼容。

## Start here / 从这里开始

- [English quick start](quickstart.md)
- [中文快速开始](zh/index.md)
- [English tutorial contents](tutorial/index.md)
- [中文教程目录](zh/tutorial/index.md)

## Documentation map / 文档地图

| Destination / 页面 | Purpose / 用途 |
| --- | --- |
| Quick Start / 快速开始 | Install MiniKafka and run one failure experiment. / 安装 MiniKafka，并运行第一个故障实验。 |
| Architecture Tour / 架构总览 | Follow one record through storage, replication, and consumption. / 跟踪一条记录如何穿过存储、复制与消费路径。 |
| MiniKafka → Kafka Mapping / 映射 | Separate equivalent mechanisms from deliberate simplifications and semantic opposites. / 区分等价机制、有意简化与语义相反。 |
| Hands-on Labs / 动手实验 | Reproduce acknowledged-write loss and consumer-group rebalance. / 复现已确认写入丢失与消费组再均衡。 |
| Differences and Evidence / 差异与证据 | Bind behavioral claims to executable tests. / 将行为声明绑定到可执行测试证据。 |

## Recommended order / 推荐顺序

Quick Start → Architecture → Mapping → Labs → Differences and Evidence

快速开始 → 架构总览 → 映射 → 动手实验 → 差异与证据

## Scope / 项目边界

MiniKafka is a teaching implementation, not a Kafka-compatible broker. Use the
mapping and evidence pages before transferring an observed behavior to
production Kafka.

MiniKafka 是教学实现，不是 Kafka 兼容代理。将观察到的行为迁移到生产 Kafka
之前，请先核对映射与证据页面。
