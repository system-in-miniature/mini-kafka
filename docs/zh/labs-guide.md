# 动手实验

先在仓库根目录安装一次：

```bash
uv sync
```

两个实验都使用临时目录和公开 Direct API，不会留下代理数据。

## 1. `acks=1` 下的领导者故障

```bash
uv run python -m minikafka.labs.leader_failure
```

预期关键输出：

```text
acknowledged offset: 0
consumer-visible end (HW): 0
records after failover: 0
```

重点区分“生产者已收到确认”和“数据已提交可见”。领导者接受了偏移量 `0`，但
跟随者尚未拉取；提升该跟随者后，未复制尾部消失。这对应 Kafka `acks=1` 表达的
持久性风险，并不表示 MiniKafka 实现了 Kafka 的选举协议。

## 2. 消费组再均衡

```bash
uv run python -m minikafka.labs.rebalance
```

预期关键输出：

```text
member 1: orders-0, orders-1, orders-2, orders-3
member 2: orders-1, orders-3
overlap after refresh: False
```

重点观察代次转换：第一个消费者最初拥有四个分区；第二个加入后，第一个消费者
仍保留旧的本地视图，直到调用 `refresh_assignment()`。刷新后的分配不再重叠。
第二个成员离开后，第一个重新拥有全部分区。MiniKafka 同步完成分配，没有 Kafka
式 revoke barrier。

## 继续查看可执行证据

这两个 lab 讲解关键权衡；完整机制由测试套件覆盖。可按
[行为矩阵](behavior-matrix.md)选择单项测试，或运行：

```bash
uv run pytest -q
```

## 下一步

使用[差异与证据](behavior-matrix.md)，将观察到的行为连接到聚焦的可执行测试。
