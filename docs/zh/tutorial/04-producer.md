# 第 4 章 · 生产者

生产者不只是把字节交给 Broker。它选择定义顺序的分区，把记录组成 batch，
决定何时 flush，跟踪确认；启用幂等后，还要给 retry 加标签，让 Broker
识别重复。MiniKafka 把这些决策放进边界清晰的小类中，可以端到端追踪。

## 学习目标

完成本章后，你应该能够：

1. 预测 keyed 与 keyless 分区选择；
2. 解释 `batch_size`、`linger_ms` 与 buffer 上限如何协作；
3. 追踪每条记录的 future 如何经过一次 batch flush；
4. 推导幂等 retry、序列缺口和 epoch fencing 的结果；
5. 精确说明 MiniKafka producer 与 Kafka producer 的差异。

## 分区决定顺序域

`src/minikafka/producer/producer.py` 的 `Producer.send` 读取 topic metadata；
若调用方未显式给分区，就委托
`src/minikafka/producer/partitioner.py::Partitioner.choose`。

非空 key 使用：

```python
zlib.crc32(key) % partition_count
```

相同 byte key 与分区数量会选择同一分区，为同一 key 提供分区内顺序。它不
保证 key 之间的顺序；改变 topic 分区数也可能改变映射。

空 key 使用 sticky 策略。partitioner 为每种 partition count 记住一个分区，
一直发送到含 keyless record 的 batch 关闭；随后
`Partitioner.on_batch_closed` 才轮到下一分区。这样相邻 keyless record 能
共享 batch，而不是每条 round-robin 成微小 batch。

该 hash **不兼容 Kafka 客户端**。Kafka 默认 producer 有自己的分区算法，
sticky 行为也经历过演进。MiniKafka 的 CRC32 规则用于确定性教学，不能用来
预测任意真实客户端的分区。

## 累积：大小、时间与有界内存

`Producer.send` 创建 `Record`、`asyncio.Future`、`PendingRecord`，并用

```python
39 + len(key or b"") + len(value or b"")
```

估算字节。`src/minikafka/producer/accumulator.py` 的
`BatchAccumulator.add` 在越过 `max_buffer_bytes` 时抛
`ProducerBufferFull`；否则按 `TopicPartition` 保存记录，记下首次入队时间，
并返回该分区估算字节是否达到 `batch_size`。

大小达到后，`Producer._schedule_flush` 启动异步
`_flush_partition`。时间达到后，`BatchAccumulator.due_partitions` 找出
最老记录已等待至少 `linger_ms` 的分区，`Producer.run_due_flushes` 再执行
flush。`Producer.flush` 无视阈值，排空所有分区和已调度任务。

只有某个动作驱动检查时，`linger_ms` 才推进。MiniKafka 没有永久 sender
线程；测试推进 `ManualClock` 后显式调用 `run_due_flushes`。这样无需 sleep
即可观察边界，但不是 Kafka 持续运行网络 sender 的模型。

### 一个 batch，多个 future

`Producer._flush_partition` 原子 pop 一个分区的 pending list，按原顺序
创建单个 `RecordBatch`。幂等 producer 会附带 PID、epoch、base sequence；
普通 producer 的这些字段为 `-1`。

集群 append 返回 batch 级 `ProduceResult`。producer 为每条记录完成
`RecordMetadata`，其 offset 为 `base_offset + delta`。`acks=0` 的结果故意
没有已知 base offset，因此 metadata offset 为 `None`，即使本教学进程已在
本地追加。

成功追加后才推进幂等序列；成功关闭 keyless batch 后才旋转 sticky 分区。
append 抛异常时，同一 batch 的每个 future 都收到相同异常。

## Broker 侧幂等

当 producer 不知道前一请求是否成功时，retry 很危险；无身份重发会追加两次。
MiniKafka 在 `src/minikafka/producer/state.py::ProducerStateManager` 中
建模 Kafka 的 PID/epoch/sequence。

状态属于每个 partition replica set，以 producer ID 为 key。
`ProducerPartitionState` 保存已接纳 epoch、最近 batch 的 last sequence 和
原始 `ProduceResult`。

`ProducerStateManager.validate` 先 fence 低于已注册或已记录 epoch 的 batch。
同 epoch 下，若候选序列范围与最近 batch 完全相同，就返回原结果而不追加，
retry 因而得到原 offset。

否则，新 epoch 的 expected base sequence 为 0；同 epoch 则为 previous last
sequence + 1。缺口、重叠或更老 retry 抛 `OutOfOrderSequence`。追加成功后，
`ProducerStateManager.record` 保存已分配 batch 和结果。

`src/minikafka/core/cluster.py::BrokerCluster.producer(idempotent=True)`
通过 `ProducerIdentityStore.allocate` 分配稳定 PID 和递增 epoch。身份文件
先写临时 JSON、flush、fsync，再原子 replace。同一 `transactional_name` 的
第二个 producer 会提高 epoch 并 fence 旧实例。MiniKafka 还强制幂等 producer
使用 `acks=all`，显式冲突模式会被拒绝。

### 重启后重建状态

`PartitionReplicaSet.__init__` 用 `leader.log.all_batches()` 构造
`ProducerStateManager`，后者对 PID 非负的存储 batch 调用 `record`。因此
精确 retry 识别在干净重启后仍成立，无需独立 producer-state checkpoint。

这种重建依赖启动时仍保留的日志，并会扫描全部 batch。生产 Kafka 使用更
丰富的 snapshot/state，避免无界重放。

## MiniKafka 与 Apache Kafka 的对照

Kafka producer 还有 serializer、metadata refresh、request size、压缩、
delivery timeout、retry/backoff、多 in-flight request 和网络 sender。
MiniKafka 直接接受 bytes，batch 不压缩，用估算 buffer 大小，显式驱动 flush，
并在进程内 append。

最大的幂等差异是重复窗口。Kafka 为每个 producer-partition 保留最近五个
batch，匹配支持的 in-flight 窗口；MiniKafka 只记最近一个精确 batch。Kafka
可能去重的更老乱序 retry，在这里会变成 `OutOfOrderSequence`。MiniKafka
事务也未建立在该序列状态上，所以第 8 章不能把它称作 Kafka 完整的 zombie
fencing 协议。

[行为矩阵](../behavior-matrix.md)链接了 keyed/sticky 分区、组批、精确 retry
与重启重建测试；[Kafka 映射](../kafka-mapping.md)给出分级与 KIP-98。

## 动手实验：组批与精确 retry

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run --offline python - <<'PY'
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.replication.model import AckMode

async def main():
    with TemporaryDirectory() as directory:
        config = MiniKafkaConfig(Path(directory))
        tp = TopicPartition("events", 0)
        retry = RecordBatch.unassigned(
            (Record(b"id", b"once", 0),),
            producer_id=77, producer_epoch=0, base_sequence=0,
        )
        async with BrokerCluster.open(config) as cluster:
            await cluster.create_topic("events", 2, 1)
            producer = cluster.producer(batch_size=4096, linger_ms=1000)
            pending = [
                producer.send(
                    "events", key=b"same", value=str(i).encode()
                )
                for i in range(3)
            ]
            await producer.flush()
            metadata = [await item for item in pending]
            print("keyed partitions:", [m.partition for m in metadata])
            print(
                "batches in keyed partition:",
                cluster.debug_batch_count(
                    TopicPartition("events", metadata[0].partition)
                ),
            )
            first = await cluster.replica_set(tp).append(retry, AckMode.ALL)
            duplicate = await cluster.replica_set(tp).append(retry, AckMode.ALL)
            print(
                "exact retry offsets:", first.base_offset,
                duplicate.base_offset, "leo:", cluster.leader_log(tp).leo
            )
        async with BrokerCluster.open(config) as reopened:
            recovered = await reopened.replica_set(tp).append(
                retry, AckMode.ALL
            )
            print(
                "restart retry offset:", recovered.base_offset,
                "leo:", reopened.leader_log(tp).leo
            )

asyncio.run(main())
PY
```

实测输出：

```text
keyed partitions: [0, 0, 0]
batches in keyed partition: 1
exact retry offsets: 3 3 leo: 4
restart retry offset: 3 leo: 4
```

三条 keyed record 都选分区 0，`flush` 形成一个 batch，占 offset 0–2。
手工序列 batch 获得 offset 3；精确 retry 返回 3，LEO 仍为 4，证明没有
第二次追加。重启后重建状态仍识别相同 retry。

## 练习

### 1. 理解题：size 与 linger

一个分区有一条低于 `batch_size` 的 pending record，手工时钟未到
`linger_ms`。本实现中哪三类公开动作仍可令其 flush？

验收：区分增加字节、推进时间和显式排空。

??? note "参考答案"

    后续 `send` 可使累计估算字节达到 `batch_size`；把时钟推进到
    `linger_ms` 后还要调用 `run_due_flushes`；`producer.flush` 或
    `producer.close` 则无视大小与时间直接排空。只推进 `ManualClock` 不会
    启动后台 sender。

### 2. 动手题：观察 sticky 轮转

创建三分区 topic 和 `batch_size=1` 的 producer，顺序发送三条 keyless
record，打印分区。

验收：分区为 `0, 1, 2`，future 全完成，且 `git diff -- src` 无输出。

??? note "参考答案"

    ```python
    producer = cluster.producer(batch_size=1, linger_ms=1000)
    metadata = [
        await producer.send("events", value=value)
        for value in (b"a", b"b", b"c")
    ]
    print([item.partition for item in metadata])
    ```

    每个单记录 batch 立即关闭，`_flush_partition` 调用
    `Partitioner.on_batch_closed` 后轮到下一分区。

### 3. 源码设计题：扩大重复窗口

不应用代码，设计每个 producer ID/epoch 保留最近五个结果，而不是一个。
覆盖数据模型、校验规则、重建行为和测试。

验收：旧精确 retry 返回各自原 offset；未知重叠仍失败；epoch fencing 不变；
内存有界。

??? note "参考答案"

    把单 result 改为最多五项的 deque，每项保存
    `(base_sequence, last_sequence, ProduceResult)`，另存 latest sequence。
    `validate` 先搜索精确范围，再强制 `latest + 1`；`record` 追加并淘汰最老
    项。启动重放自然重建 deque。测试覆盖六个 batch、五个保留项的 retry、
    已淘汰最老项被拒、重启，以及高 epoch 从 sequence 0 开始并 fence 旧
    epoch。

## 小结

生产者在存储看到记录前就决定顺序域：key 稳定选分区，keyless record
sticky 以利组批。size、linger 与 buffer 上限决定何时形成 `RecordBatch`，
future 保留每条记录的结果。PID、epoch、sequence 让 Broker 为精确 retry
返回原结果并拒绝歧义顺序。第 5 章将沿着这些 batch 进入 follower，研究
LEO、高水位以及 `acks=0/1/all` 如何把追加变成耐久性选择。
