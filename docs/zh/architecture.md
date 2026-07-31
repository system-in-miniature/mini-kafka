# 架构总览

MiniKafka 将传输层放在语义核心之外。大多数实验直接调用 Python API，因此无需
套接字也能观察主要路径：

```text
Producer
  -> 按键选择分区
  -> 记录批次
  -> 领导者 PartitionLog
  -> 跟随者拉取 / ISR
  -> 高水位
  -> Consumer 或 ConsumerGroup 位置
```

存储层负责仅追加日志段、稀疏偏移量索引、CRC 恢复、保留与按键压缩。
`BrokerCluster` 组装主题元数据、生产者状态、消费组协调、副本集合和事务可见性；
`JsonTcpServer` 只是建立在相同操作之上的有界换行 JSON 适配器。

建议按以下顺序阅读源码：

1. `log/`：批次、日志段、索引、恢复、保留与压缩。
2. `producer/`：分区、批处理、确认与去重。
3. `consumer/`：位置、消费组所有权、代次与隔离。
4. `replication/`：跟随者拉取、ISR、高水位、任期与提升。
5. `transaction/`：事务标记、隔离和“输出 + 偏移量”原子提交。
6. `broker.py`、`cluster.py`、`lifecycle.py`：所有权与关闭。
7. 最后读 `adapters/`，因为传输不定义核心语义。

下一章给出分级的 [MiniKafka → Apache Kafka 映射](kafka-mapping.md)；
[行为矩阵](behavior-matrix.md)则把可观察声明连接到测试证据。

## 下一步

继续阅读 [MiniKafka → Apache Kafka 映射](kafka-mapping.md)，区分与源码同形的
核心不变量和教学项目有意保留的简化。
