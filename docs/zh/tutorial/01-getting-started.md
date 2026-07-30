# 第 1 章 · 认识 MiniKafka

MiniKafka 是一个小型、可执行的 Kafka 数据平面模型。它不是缩小版生产
Broker，也不是只返回方便答案的 mock：日志确实持久化 batch，生产者确实
选择分区并组批，追随者确实从 leader 拉取，消费者确实停在高水位，恢复
逻辑也确实会修复损坏的活跃段尾部。实现足够小，可以在一次学习中追完；
边界又足够真实，可以做有意义的故障实验。

## 学习目标

完成本章后，你应该能够：

1. 说明 MiniKafka 建模了什么、主动省略了什么；
2. 通过 direct API 创建集群和 topic；
3. 把一条记录从 `Producer.send` 追到 `BrokerCluster.fetch`；
4. 区分 topic、partition、offset、LEO 和高水位；
5. 知道后续每个核心机制由哪一章负责。

## 为什么先读可执行教学内核

需要生产细节时，Apache Kafka 源码无可替代，但它不是容易使用的第一台
显微镜。一条 Produce 请求会穿过网络协议、请求分发、鉴权、配额、元数据、
复制、存储、指标以及多层兼容逻辑。若只是随意删掉这些层，得到的往往是
玩具队列，Kafka 之所以具有当前语义的原因也会一起消失。

MiniKafka 采取更窄的路线。仓库 `README.md` 把它称为 “direct-first
reference implementation”。direct-first 表示测试和实验在同一 Python
进程中直接调用领域对象；它**不**表示持久化与复制是假的。JSON/TCP 是独立
的薄适配边界，第 10 章才处理它。direct API 让我们先以确定性方式观察领域
状态，再引入传输问题。

中心对象是 `src/minikafka/core/cluster.py` 的 `BrokerCluster`。
`BrokerCluster.open` 加载元数据，为每个已分配副本打开一个
`PartitionLog`，恢复消费者已提交位点，并重建 replica set。
`BrokerCluster.create_topic` 校验 topic 名，计算轮转副本分配，打开相应
日志目录，最后才发布元数据。这些都是真实的存储与所有权步骤，并不是内存
字典假扮 Broker。

### 第一组词汇

**Topic** 是命名的分区集合。**Partition** 是排序和复制的单位。同一分区
中的记录获得单调递增的 **offset**，分区之间没有总序。因此 offset 是
`(topic, partition)` 内的地址，而不是全局消息 ID。

**日志末端位点**（LEO）是某个副本下一次写入的位置。若已存 offset
0、1、2，则 LEO 为 3。**高水位**（HW）是消费者可见的已复制前缀末端。
单副本分区追加后 HW 会立即跟随 LEO；多副本分区的 leader 可能存在尚未
复制的尾部，因此会出现 `LEO > HW`。第 5 章会让这段差值直接可见。

值的路径从 `src/minikafka/producer/producer.py` 开始。
`Producer.send` 查 topic 元数据，选择或校验分区，创建 `Record`，放入
`BatchAccumulator`，返回 `asyncio.Future`。batch 就绪后，
`Producer._flush_partition` 构造一个 `RecordBatch` 并调用
`BrokerCluster.append_batch`，再把 batch 结果转换为每条记录的
`RecordMetadata`。

`BrokerCluster.append_batch` 委托给
`src/minikafka/replication/replica_set.py` 的
`PartitionReplicaSet.append`。它检查生产者序列和确认策略，把 batch
追加到 leader 的 `PartitionLog`，并推进复制可见状态。
`BrokerCluster.fetch` 从 leader 读取，但止于 `visible_end`，也就是 replica
set 的高水位。因此即使最简单的 produce→fetch，也经过了分区、组批、复制
日志抽象和可见性边界。

### 为什么必须使用上下文管理器

示例使用：

```python
async with BrokerCluster.open(config) as cluster:
    ...
```

`BrokerCluster.__aexit__` 会调用 `BrokerCluster.close`。关闭生产者时会
flush accumulator，日志被 flush 并关闭，归属该集群的后台任务也会 join。
这属于正确性，而不只是代码风格。若直接丢弃集群对象，就无法判断待处理
batch 是故意丢失、仍在内存，还是已经持久化。后续故障实验需要不同语义时，
会显式调用 `BrokerCluster.crash`。

## MiniKafka 与 Apache Kafka 的对照

二者是概念对应，不是 wire compatibility。真实 Kafka 客户端需要发现
Broker、序列化 Kafka 协议请求、协商 API 版本并走网络。MiniKafka direct
adapter 跳过传输工作，直接进入测试也使用的领域核心。

真实 Kafka 还把 Broker 分布在多个进程或机器上，并由 KRaft controller
quorum 管理元数据与选举。MiniKafka 用本地 JSON 保存元数据，在单进程中
表示多个 Broker，并显式执行 promotion。它没有 TLS、SASL、ACL、配额、
机架感知、分层存储、Kafka Connect 或 Kafka Streams；磁盘 batch 是自定义、
不压缩的 `MKB1` frame，而不是 Kafka 的 wire/log 格式。

这些省略是边界，不是脚注。请用[行为矩阵](../behavior-matrix.md)查看可执行
证据，用 [MiniKafka → Kafka 映射](../kafka-mapping.md)查看分级对应关系和
真实 Kafka 资料。尤其要注意：“direct API 成功”只证明教学内核的领域路径，
不能证明它兼容 `kafka-python`、Java producer 或真实 Kafka 集群。

Direct API 还带来一个教学优势：时间、复制轮次和故障点都可由实验控制。
你能在 leader 已写入、follower 尚未拉取的精确时刻观察状态，而不用依赖
操作系统调度和网络竞态。代价是性能数字和网络行为不能外推到生产 Kafka。
本书会把“机制证据”和“生产等价性”始终分开。

## 动手实验：第一条记录

在仓库根目录执行。受限环境中默认用户缓存可能只读，因此命令显式指定
cache；这不会改变 MiniKafka 行为。

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run --offline python - <<'PY'
import asyncio
from tempfile import TemporaryDirectory
from pathlib import Path
from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition

async def main():
    with TemporaryDirectory() as directory:
        async with BrokerCluster.open(MiniKafkaConfig(Path(directory))) as cluster:
            await cluster.create_topic("orders", partitions=2, replication_factor=1)
            producer = cluster.producer(batch_size=1)
            metadata = await producer.send(
                "orders", key=b"alice", value=b"created"
            )
            records = cluster.fetch(
                TopicPartition("orders", metadata.partition), 0, 10
            )
            print(f"partition={metadata.partition} offset={metadata.offset}")
            print([(r.offset, r.key, r.value) for r in records])

asyncio.run(main())
PY
```

实测输出：

```text
partition=1 offset=0
[(0, b'alice', b'created')]
```

分区为 1，是因为 `src/minikafka/producer/partitioner.py` 的
`Partitioner.choose` 计算 `zlib.crc32(b"alice") % 2`。offset 为 0，是
因为它是该分区第一次追加。`batch_size=1` 让记录立即 flush。返回元数据
告诉我们写入位置；在分区系统中猜一个分区再 fetch 是错误做法。

临时目录保证每次运行都从空元数据和空日志开始，因此输出可复现。若要研究
重启，应改用稳定路径，关闭第一个集群后再打开第二个。

## 练习

### 1. 理解题：地址与身份

为什么实验中的 offset 0 不足以唯一定位记录？写出公开 API 所表达的最小
完整地址。

验收：答案必须列出全部地址分量，并解释两条记录为何都可能是 offset 0。

??? note "参考答案"

    完整地址是 `(topic, partition, offset)`，本例为
    `("orders", 1, 0)`。每个分区都有自己的 offset 序列，`orders` 的另一个
    分区也能有 offset 0，其他 topic 同样可以。

### 2. 动手题：证明同 key 的确定性

只改内联实验，不改仓库源码：用相同 key 发送三个值，等待三个 future，
打印其分区。使用较大 batch size，调用 `await producer.flush()`，验证分区
相同且 offset 保持发送顺序。

验收：分区集合大小为 1；fetch 值依次为 `b"0"`、`b"1"`、`b"2"`；执行
`git diff -- src` 无输出。

??? note "参考答案"

    把单次 send 替换为：

    ```python
    producer = cluster.producer(batch_size=4096, linger_ms=1000)
    pending = [
        producer.send("orders", key=b"alice", value=str(i).encode())
        for i in range(3)
    ]
    await producer.flush()
    metadata = [await future for future in pending]
    print({item.partition for item in metadata})
    ```

    从 `metadata[0].partition` fetch。集合为 `{1}`，而
    `_flush_partition` 保留 accumulator 顺序。

### 3. 源码设计题：暴露 LEO 与 HW

起草但不要应用一个 patch：给 `BrokerCluster` 增加只读
`describe_offsets(tp)`，返回 leader LEO 与 visible end。指出它应委托给
哪些现有函数，并提出一个测试。

验收：草案不新增存储状态；复用已有对象；测试在双副本 topic 的 follower
拉取前证明 `LEO > HW`。

??? note "参考答案"

    最小设计是：

    ```diff
    +def describe_offsets(self, tp: TopicPartition) -> tuple[int, int]:
    +    return self.leader_log(tp).leo, self.visible_end(tp)
    ```

    测试用 `acks=1` 追加后断言 `(1, 0)`，调用
    `replica_set.fetch_followers_once()` 后断言 `(1, 1)`。状态已经分别由
    `PartitionLog.leo` 和 `PartitionReplicaSet.high_watermark` 所有。

## 小结

MiniKafka 是可执行语义模型：足够小，却仍拥有真实分区日志、生产者 batch、
副本状态和重启行为。direct API 去掉传输噪声而保留这些机制。第一条记录
获得了分区与 offset，并通过高水位边界变得可见。第 2 章将打开这份分区
日志，研究 segment、稀疏索引、CRC frame 与启动恢复——后续一切保证的
存储地基。
