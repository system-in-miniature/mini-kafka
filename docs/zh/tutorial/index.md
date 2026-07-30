# MiniKafka 教程

这套十章教程把 MiniKafka 当作 Kafka 特色数据平面机制的可执行教学内核。
建议按顺序阅读：存储先建立 offset 模型，生产者与复制在其上构建，消费组
与事务再加入协调和可见性边界。

每章都包含回到具体源码的机制讲解、与 Apache Kafka 的明确对照、贴有实测
输出的实验，以及带验收标准的练习。MiniKafka 不兼容 Kafka wire protocol；
请结合参考资料，始终区分“已经建模的语义”和“生产系统等价性”。

## 章节

1. [认识 MiniKafka](01-getting-started.md)——教学内核边界、环境以及 direct
   API 的第一条 produce→fetch。
2. [日志：一切的地基](02-the-log.md)——segment 滚动、稀疏 offset 索引、
   CRC frame 和 active-tail 恢复。
3. [保留与压实](03-retention-compaction.md)——前缀 retention、keyed
   compaction、tombstone 和目录替换。
4. [生产者](04-producer.md)——keyed/sticky 分区、batch、linger，以及
   PID/epoch/sequence 幂等。
5. [复制 I：追随与水位](05-replication-basics.md)——follower 拉取、LEO、
   HW、可见性和 acknowledgement 模式。
6. [复制 II：ISR 与栅栏](06-isr-and-fencing.md)——ISR 收缩恢复、minimum
   ISR、leader epoch 与 promotion。
7. [消费组](07-consumer-groups.md)——generation fencing、心跳、分配、
   offset commit 与 rebalance 边界。
8. [事务](08-transactions.md)——journal 恢复、marker、LSO、
   `read_committed` 与原子 offset 发布。
9. [投递语义实验课](09-delivery-semantics.md)——已确认写丢失、幂等 retry，
   以及 exactly-once 的组成要件。
10. [协议层与方法论](10-protocol-and-beyond.md)——薄 TCP adapter、wire
    protocol 差异，以及继续阅读真实 Kafka 的路线。

## 参考资料

- [快速开始与项目概览](../index.md)
- [架构](../architecture.md)
- [MiniKafka → Kafka 映射](../kafka-mapping.md)
- [动手实验](../labs-guide.md)
- [行为矩阵与可执行证据](../behavior-matrix.md)

英文正典见 [English tutorial contents](../../tutorial/index.md)。
