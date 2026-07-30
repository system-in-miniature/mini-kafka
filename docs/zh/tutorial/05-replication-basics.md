# 第 5 章 · 复制 I：追随与水位

复制把“leader 接受了 batch”和“已复制日志可以安全暴露 batch”分开。
MiniKafka 为每个 topic-partition 建立一个 `PartitionReplicaSet`。follower
从自己的 LEO 拉取完整 batch，高水位标出共同的已提交前缀，生产者的确认
模式则决定要等待多少证据。

## 学习目标

完成本章后，你应该能够：

1. 从 follower LEO 追踪一次 follower-fetch 到 batch 追加；
2. 区分每个副本的 LEO 与分区高水位；
3. 解释消费者为何不能读 leader-only tail；
4. 预测 `acks=0`、`acks=1`、`acks=all` 的结果与风险；
5. 演示已确认的 `acks=1` 写入如何在 promotion 后丢失。

## Replica set 状态

`src/minikafka/core/cluster.py::BrokerCluster.create_topic` 为每个分区分配
副本 Broker ID，并为每个 assignment 打开一个 `PartitionLog`。
`BrokerCluster._make_replica_set` 把日志包装成 `Replica`，再构造
`src/minikafka/replication/replica_set.py::PartitionReplicaSet`。

每个 `Replica` 的 `Replica.leo` 委托给底层 `PartitionLog.leo`。replica set
拥有 leader ID/epoch、副本 map、ISR、高水位、待完成 `acks=all` waiter 和
Broker 侧生产者序列状态。

初始化时，LEO 与 leader 相等的副本进入 ISR，leader 总会加入。初始高水位
是所有已分配副本 LEO 的最小值。MiniKafka 的重启重建比 Kafka 简单：它从
当前副本日志推导 HW，而不是读取复制 checkpoint 文件。

## Follower 拉取，leader 不推送

`PartitionReplicaSet.fetch_followers_once` 表示一轮确定性复制。在 replica
set 锁内遍历 follower。若 follower 比 leader 更长，先截到当前高水位；
然后调用：

```python
leader.log.read_batches(
    follower.leo,
    config.replica_fetch_max_bytes,
)
```

返回 batch 已带 base offset 和 leader epoch。
`follower.log.append_replica_batch` 要求 base 等于 follower LEO，从而保持
连续。随后更新 `last_fetch_ms`，调用 `refresh_isr` 重算 membership 和 HW。

复制单位是完整 batch，不是单条记录。`read_batches` 即使第一个 batch 超过
byte budget 也会返回它，但不会再加入会越界的后续 batch；这样既能前进，
又不拆 CRC frame。

MiniKafka 没有自动网络 fetcher。实验显式调用 `fetch_followers_once`，或用
`BrokerCluster.replicate_all_once` 推进所有分区。这使我们能精确暂停在
leader append 与 follower replication 之间，观察不安全尾部。

## LEO、ISR 与高水位

LEO 是本地状态：某个副本下一次写入位置。HW 是共享可见性：replica set 已
声明提交的最大前缀。`PartitionReplicaSet._advance_high_watermark` 计算
当前 ISR 成员 LEO 的最小值，再执行：

```python
self.high_watermark = max(self.high_watermark, candidate)
```

`max` 使 HW 在一个 leader epoch 内单调。临时落后或刚被评估的成员不能把
已提交边界向后拖。

`refresh_isr` 要求 follower 同时满足：

1. offset lag 不超过 `replica_lag_max_offsets`；
2. 上次 fetch 距今不超过 `replica_lag_time_ms`；
3. LEO 覆盖当前 HW。

第三条防止重新加入导致已提交前缀收缩。显式 offset-lag 阈值是教学控制，
类似 Kafka 已移除的 `replica.lag.max.messages`；当前 Kafka 主要看时间 lag。
第 6 章会深入 ISR 移出、恢复、minimum ISR 与 epoch。

### 可见性止于 HW

`PartitionReplicaSet.fetch` 在 `read_uncommitted` 下仍把
`end_offset=self.high_watermark` 传给 `PartitionLog.fetch`。这里的
“uncommitted” 是第 8 章的事务隔离术语，不表示能读未复制字节。

`BrokerCluster.fetch` 同样使用 `BrokerCluster.visible_end` 返回的 HW。
因此消费者看不到 leader-only record；follower 追上并推进 HW 后，同一
offset 才可读，记录本身无需变化。

## Acknowledgement 到底承诺什么

`src/minikafka/replication/model.py::AckMode.parse` 接受 `0`、`1`、`all`，
也把 Kafka 的 `-1` 归一成 `all`。
`PartitionReplicaSet.append` 在校验生产者状态后实现三种模式。

### `acks=0`：不知道 offset

在这个进程内模型中 leader 仍会追加，但 `ProduceResult.base_offset` 为
`None`，`offsets_known` 为 false，最终 `RecordMetadata.offset=None`。这只
建模 fire-and-forget 的响应语义，不是现实网络 send buffer 或静默 Broker
故障模型。

### `acks=1`：仅 leader 本地确认

leader 追加并立即返回 offset，不等 follower；HW 可能不变。若复制前 leader
失败，在 HW 处提升 follower 会丢掉已确认尾部。下方实测会直接展示。

### `acks=all`：等待捕获的 ISR

追加前，ISR size 必须达到 `min_insync_replicas`，否则抛
`NotEnoughReplicas` 且不写。leader append 后，`AckWaiter` 捕获当前 ISR 为
`acknowledgement_set`，目标为 batch next offset；只有捕获成员的 LEO 全部
达到目标，future 才完成。

若等待期间 ISR 低于 minimum，`_resolve_ack_waiters` 抛
`NotEnoughReplicasAfterAppend`。注意此时 batch 已在 leader，客户端却得到
错误；retry 必须依靠幂等才能避免重复。

`acks=all` 不是“永远等待全部配置副本”，而是“等待本次捕获的 ISR，并满足
minimum ISR”。第 6 章会继续讨论 ISR 质量与选举策略如何决定这句话的强度。

## MiniKafka 与 Apache Kafka 的对照

真实 Kafka follower 同样从 leader fetch，leader 跟踪副本进度，HW 限制
消费者可见性，producer `acks` 与 `min.insync.replicas` 联合作用。

MiniKafka 的所有副本位于单进程，只有调用方 pump 才推进复制；它没有
Broker 间网络、fetch session、限速、controller 自动选举、机架布局或复制
checkpoint。promotion 是手工的。尤其是它在 failover 时截到 HW，甚至可能
截断**被提升副本自身**；Kafka KIP-101 的 leader-epoch reconciliation 把
当选 leader 视为事实源，让 follower 向它截断，两者在该边缘上的方向相反。

[行为矩阵](../behavior-matrix.md)链接 follower fetch、HW、ack 和已确认写
丢失测试；[Kafka 映射](../kafka-mapping.md)记录启动与 promotion 差异。
本确定性模型中 `acks=all` 成功，不能外推成生产 failover 已被证明安全。

## 动手实验：安全前缀与丢失尾部

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run --offline python - <<'PY'
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition

async def main():
    with TemporaryDirectory() as directory:
        config = MiniKafkaConfig(
            Path(directory), broker_ids=(1, 2), min_insync_replicas=2
        )
        async with BrokerCluster.open(config) as cluster:
            await cluster.create_topic("events", 1, 2)
            tp = TopicPartition("events", 0)
            replica_set = cluster.replica_set(tp)
            safe_producer = cluster.producer(batch_size=1, acks="all")
            pending = safe_producer.send("events", value=b"safe")
            await asyncio.sleep(0)
            print(
                "acks=all pending:", not pending.done(),
                "HW:", replica_set.high_watermark
            )
            await replica_set.fetch_followers_once()
            safe = await pending
            print(
                "acks=all offset:", safe.offset,
                "LEOs:", [replica_set.replicas[i].leo for i in (1, 2)],
                "HW:", replica_set.high_watermark
            )

            risky_producer = cluster.producer(batch_size=1, acks=1)
            risky = await risky_producer.send(
                "events", value=b"at-risk"
            )
            print(
                "acks=1 offset:", risky.offset,
                "visible end:", cluster.visible_end(tp)
            )
            await cluster.promote(tp, 2)
            print(
                "after promotion:",
                [r.value for r in cluster.fetch(tp, 0, 10)]
            )

asyncio.run(main())
PY
```

实测输出：

```text
acks=all pending: True HW: 0
acks=all offset: 0 LEOs: [1, 1] HW: 1
acks=1 offset: 1 visible end: 1
after promotion: [b'safe']
```

第一次 send 在 leader append 后仍 pending，因为 follower LEO 与 HW 都是 0。
一轮 follower fetch 复制 batch，两个 LEO 和 HW 都到 1，waiter 完成。

第二次 send 从 leader 得到 offset 1，但 visible end 仍为 1：offset 1 恰好
位于可读前缀之外。随后 broker 2 在 safe end 1 被提升，它只有 `safe`，
所以已确认的 `at-risk` 消失。这是对 `acks=1` 承诺边界的受控证明，不是
随机故障模拟。

## 练习

### 1. 理解题：“commit”的两种含义

为什么 `read_uncommitted` 仍不能读 HW 以上？这与事务 commit 有何不同？

验收：分别指出复制边界和事务隔离边界。

??? note "参考答案"

    HW 是所有消费者共有的复制前缀边界，任何 isolation 都不能越过。
    `read_uncommitted` 可以包含事务结果未决或已 abort 的数据；
    `read_committed` 还会止于 last stable offset 并过滤 aborted transaction。
    “Uncommitted” 不等于 “leader-only”。

### 2. 动手题：观察 `acks=0`

在单副本集群创建 `acks=0` producer，发送一个值，打印 metadata offset、
leader LEO 与 fetch 结果。

验收：metadata offset/base offset 均为 `None`，LEO 为 1，记录可 fetch，
且 `git diff -- src` 无输出。

??? note "参考答案"

    ```python
    producer = cluster.producer(batch_size=1, acks=0)
    metadata = await producer.send("events", value=b"fire-and-forget")
    print(metadata.offset, metadata.base_offset)
    print(cluster.leader_log(tp).leo)
    print([r.value for r in cluster.fetch(tp, 0, 10)])
    ```

    输出为 `None None`、`1`、`[b'fire-and-forget']`。这只证明进程内 append；
    真实 `acks=0` 客户端收不到 Broker Produce response，可能错过传输或
    Broker 错误。

### 3. 源码设计题：自动 follower loop

不应用代码，设计周期调用 `fetch_followers_once` 的生命周期。覆盖 shutdown、
crash、失败传播与确定性测试。

验收：loop 有明确 owner 且可 join，不隐藏 terminal exception，测试不依赖
墙钟 sleep。

??? note "参考答案"

    可以让集群拥有每个 replica set 的 task，或拥有一个 scheduler task；
    它等待可注入 clock/tick，调用 follower fetch，并通过现有 lifecycle/
    failure-injector 边界记录 terminal failure。`close` 请求停止、await task，
    再 flush 日志；`crash` 直接 cancel，不做优雅 drain。测试用 manual tick
    断言 LEO/HW 转移，注入 fetch 错误，并断言后续公开操作暴露 terminal
    error。自由运行的 `asyncio.sleep` loop 会破坏当前确定性实验。

## 小结

Follower 从自身 LEO 复制完整 batch，HW 标记消费者可见的已复制前缀。
`acks=0` 不返回 offset 知识，`acks=1` 只确认 leader append，
`acks=all` 在 minimum ISR 约束下等待捕获的 ISR。丢尾实验把这些术语变成
可观察状态。第 6 章将继续进入最难边界：ISR 收缩与恢复、minimum-ISR
失败、leader epoch、promotion 和 fencing。
