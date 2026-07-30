# 第 6 章 · 复制 II：ISR 与栅栏

上一章建立了物理图景：leader 追加，follower 拉取，高水位（HW）限制普通读取。本章追问更难的控制面问题：哪些副本足够安全，可以计入当前持久性承诺？何时必须拒绝写入？新 leader 又如何让发往旧 leader 的请求失效？

## 学习目标

学完本章，你将能够：

- 根据 follower 的时间滞后、offset 滞后和当前 HW 预测 ISR 成员；
- 区分 `acks=all` 在追加前与追加后的两类失败；
- 解释为什么 `min.insync.replicas` 只有和 `acks=all` 配合才有意义；
- 追踪 promotion、leader epoch 持久化和陈旧请求栅栏；以及
- 指出 MiniKafka 故障转移语义与 Kafka 的差异。

## ISR 是承诺，而不是副本列表

每个已分配副本都有存储，但只有**同步副本**（in-sync replica）参与当前持久性边界。`src/minikafka/replication/replica_set.py` 的 `PartitionReplicaSet.__init__` 在打开副本集时，把 LEO 等于 leader LEO 的副本放入 ISR，并始终加入 leader。它还把 HW 初始化为所有副本 LEO 的最小值。这些是教学模型的启动规则，并不是持久化的 Kafka controller 决策。

实时规则直接写在 `src/minikafka/replication/replica_set.py` 的 `PartitionReplicaSet.refresh_isr` 中：

```python
within_offset = leader_leo - follower.leo <= replica_lag_max_offsets
within_time = now - follower.last_fetch_ms <= replica_lag_time_ms
has_committed_prefix = follower.leo >= self.high_watermark
```

follower 必须同时满足三个谓词。前两个表示它在记录数量上没有落后太远、最后拉取时间也没有过旧。第三个是安全闸门：不包含既有已提交前缀的副本不能重新进入 ISR，否则把它更小的 LEO 纳入 HW 最小值，可能会和此前已允许 consumer 看到的数据发生矛盾。

章名中的“双条件”强调收缩和恢复在推理上有不同含义：当前 ISR follower 违反滞后条件就会退出；已退出 follower 只有在一次拉取之后既及时、足够接近，**又**覆盖 HW，才能恢复。MiniKafka 用一次集合重算表达这些谓词，但区分“退出”和“安全重入”能避免常见误解：“follower 发过心跳，所以安全了。”及时但为空的 follower 依然不安全。

follower 工作被刻意做成显式步骤。`src/minikafka/replication/replica_set.py` 的 `PartitionReplicaSet.fetch_followers_once` 从各 follower 的 LEO 开始复制完整 batch，更新 `last_fetch_ms`，再调用 `refresh_isr`。这里没有后台复制线程。因此每次状态转换都能用 `ManualClock` 重现，但实验也必须显式调用推进复制的步骤。

成员变化后，`PartitionReplicaSet._advance_high_watermark` 计算当前 ISR 各副本 LEO 的最小值，并取 `max(old_hw, candidate)`。所以在一个正在运行的副本集实例中，HW 是排他的、单调不减的边界。`src/minikafka/core/cluster.py` 的 `BrokerCluster.fetch` 把 `BrokerCluster.visible_end` 返回的 HW 作为普通读取终点。

## `acks=all` 有两个失败窗口

写路径位于 `src/minikafka/replication/replica_set.py` 的 `PartitionReplicaSet.append`。检查调用者提供的 leader epoch 和幂等 producer 状态后，它先执行前置条件：

```python
if mode is AckMode.ALL and len(self._isr) < min_insync_replicas:
    raise NotEnoughReplicas(...)
```

这是**追加前**失败，leader 日志尚未改变。ISR 恢复后可以直接重试。相反，`acks=1` 和 `acks=0` 不做这项检查：`min.insync.replicas` 不会神奇地让这些模式变得持久。

对于 `acks=all`，追加完成后会把当时的 ISR 捕获到 `AckWaiter`。只有该 acknowledgement set 中的每个副本都到达 batch 的 next offset，future 才完成。捕获集合很重要：不能仅仅让某个 follower 退出 ISR，就让同一个请求变得更容易满足。

还有一类**追加后**失败。`PartitionReplicaSet._resolve_ack_waiters` 发现请求等待期间 ISR 低于 `min.insync_replicas` 时，会用 `NotEnoughReplicasAfterAppend` 结束 future。记录可能已经存在于 leader 上，所以客户端面对的是未知结果，必须以安全方式重试——通常要依赖幂等性。这是一条普遍的分布式系统规律：错误响应不等于状态一定没有改变。

`src/minikafka/errors.py` 分别定义了 `NotEnoughReplicas` 与 `NotEnoughReplicasAfterAppend`。保留这种区分能显式呈现结果歧义，而不是把两者藏进一个普通超时。

## Leader epoch 把旧权威变成可检测错误

ISR 成员限制 promotion 候选。`src/minikafka/replication/replica_set.py` 的 `PartitionReplicaSet.promote` 会以 `NotInSyncReplica` 拒绝 ISR 之外的 broker。对于合法候选，它把当前 HW 保存为 `safe_end`，必要时把候选截断到这一边界，更改 `leader_id`，递增 `leader_epoch`，把 ISR 重置为新 leader，并用 `FencedLeaderEpoch` 结束所有待确认 waiter。

随后，`src/minikafka/core/cluster.py` 的 `BrokerCluster.promote` 通过 metadata store 写入新 leader ID 与 epoch。重启时，`BrokerCluster.open` 把该 epoch 交给各 `PartitionLog`，所以新权威能够跨进程生命周期保存。每个追加 batch 也会通过 `src/minikafka/core/batch.py` 的 `RecordBatch.assign` 获得当前 epoch。

最后，`PartitionReplicaSet.append` 接受可选的 `leader_epoch`。调用者如果提供旧 epoch，方法会在 producer 校验和日志追加之前拒绝请求。这就是**栅栏**：正确性来自比较权威世代，而不是祈祷旧 leader 已经停止运行。

MiniKafka 的公开 producer 没有实现 metadata refresh 循环，因此显式 epoch 参数主要是白盒教学接口。但其不变量仍然具有生产意义：任何安全性依赖 leader 的命令，都必须携带或推导出它所相信的 leader 世代。

## 与真实 Kafka 对照

把本模型迁移到生产思维之前，请阅读仓库的[复制与故障转移映射](../kafka-mapping.md)和[复制证据行](../behavior-matrix.md)。

以下区别不可省略：

- 现代 Kafka 主要使用 `replica.lag.time.max.ms` 判断 ISR 活性。MiniKafka 还使用显式 offset 上限，类似已移除的 `replica.lag.max.messages`，以支持确定性实验。
- Kafka controller（在当前部署中通常是 KRaft quorum）协调选举与元数据；MiniKafka promotion 是进程内显式方法。
- Kafka broker 是不同故障域；MiniKafka 虽把副本放在不同目录，却运行于同一进程。
- Kafka 使用 leader-epoch 历史进行分歧协调（KIP-101、KIP-279），MiniKafka 没有实现。
- 最重要的是，MiniKafka 的 `promote()` 会把候选 leader 截断到旧 HW。真实 Kafka 以当选 leader 的 LEO 为端点，让 follower 与之对齐。映射文档将 MiniKafka 的行为评为**语义相反**，因为它可能丢弃在 HW 推进前已经存在于全部 ISR 副本的数据。应当用它研究安全边界，不能把它当成 Kafka 选举算法。

`acks=all` waiter 也比 Kafka 复制机制小得多。MiniKafka 等待追加时捕获的每个 ISR 成员；Kafka 的完成条件整合在 ISR 与 HW 演进中。可迁移的是 acknowledgement mode、ISR 与 `min.insync.replicas` 的契约，不是完全相同的调度算法。

这个差别也解释了阅读教学内核的正确姿势：先用短代码精确看见状态转换，再到真实系统中寻找相同不变量的分布式所有权、持久化格式和后台调度。教学模型减少参与者数量，却不能替真实系统决定哪些故障是可能的。

在运维层面，应把 ISR 大小和滞后与 acknowledgement error 一起监控。看似健康的请求速率，可能掩盖某个分区已经只剩一个同步副本。因此，`NotEnoughReplicas` 不只是应用错误；它还表示所请求的持久性契约当前不存在可接受的执行路径。

## 动手实验：收缩、恢复与栅栏

在仓库根目录运行以下命令。实验使用临时目录和 Direct API，不打开 socket，也不遗留数据。

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run python - <<'PY'
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition
from minikafka.clock import ManualClock
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.errors import FencedLeaderEpoch, NotEnoughReplicas
from minikafka.replication.model import AckMode

async def main():
    with TemporaryDirectory() as d:
        config = MiniKafkaConfig(
            data_dir=Path(d), broker_ids=(1, 2),
            min_insync_replicas=2, replica_lag_max_offsets=0,
        )
        async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
            await cluster.create_topic("events", 1, 2)
            tp = TopicPartition("events", 0)
            rs = cluster.replica_set(tp)
            print("initial ISR:", sorted(rs.isr))
            await rs.append(
                RecordBatch.unassigned((Record(None, b"one", 0),)),
                AckMode.LEADER,
            )
            rs.refresh_isr()
            print("after leader-only append:", sorted(rs.isr))
            try:
                await rs.append(
                    RecordBatch.unassigned((Record(None, b"two", 0),)),
                    AckMode.ALL,
                )
            except NotEnoughReplicas as e:
                print("acks=all precheck:", e.code)
            await rs.fetch_followers_once()
            print("after follower catch-up:", sorted(rs.isr),
                  "HW:", rs.high_watermark)
            old_epoch = rs.leader_epoch
            await cluster.promote(tp, 2)
            print("promoted leader/epoch:", rs.leader_id, rs.leader_epoch)
            try:
                await rs.append(
                    RecordBatch.unassigned((Record(None, b"stale", 0),)),
                    AckMode.LEADER, leader_epoch=old_epoch,
                )
            except FencedLeaderEpoch as e:
                print("stale request:", e.code)

asyncio.run(main())
PY
```

实测输出：

```text
initial ISR: [1, 2]
after leader-only append: [1]
acks=all precheck: NOT_ENOUGH_REPLICAS
after follower catch-up: [1, 2] HW: 1
promoted leader/epoch: 2 1
stale request: FENCED_LEADER_EPOCH
```

第一次追加制造了一个 offset 的滞后；因为允许上限为零，broker 2 退出 ISR。追赶后，三个重入谓词全部恢复。promotion 递增 epoch，之后再提交 epoch 0 会被拒绝。

## 练习

1. **理解题：** 为什么“follower LEO 接近 leader LEO”不足以允许 ISR 重入？

    ??? note "参考答案"

        它没有说明此前已提交前缀是否存在，也没有说明拉取是否足够新。因此 MiniKafka 同时要求 offset 接近、时间接近和 `follower.leo >= high_watermark`。没有 HW 闸门，加入 follower 可能让持久性边界和已经暴露给 consumer 的数据矛盾。

2. **动手题：** 把实验复制到 `/tmp/isr_lab.py`，把 `min_insync_replicas` 改为 `1`，预测 `acks=all` 前置检查是否仍失败。不要修改 `src/`。验收：脚本为第二次追加打印 offset，而不是 `NOT_ENOUGH_REPLICAS`。

    ??? note "参考答案"

        ISR 为 `{1}` 时，其大小等于新的最小值，前置检查通过。捕获的 acknowledgement set 只有 broker 1，所以 leader 追加后 future 完成。这是合法配置，但此刻只有单副本持久性。

3. **动手设计题：** 在 `/tmp` 写一个测试：发起 `acks=all`，在 leader 追加后移除一个 ISR follower，并断言 `NotEnoughReplicasAfterAppend`。验收：`uv run pytest -q /tmp/test_post_append.py` 输出 `1 passed`。

    ??? note "参考答案"

        用 `asyncio.create_task` 启动 `rs.append(..., AckMode.ALL)`，让出事件循环一次，使 leader 追加和 waiter 建立，再在 `min_insync_replicas=2` 时调用 `rs.remove_from_isr(2)`。在 `pytest.raises(NotEnoughReplicasAfterAppend)` 中等待 task。这样无需改实现，就验证了未知结果分支。

## 小结

ISR 是有资格定义当前持久性承诺的集合，而不只是副本分配表。MiniKafka 用紧凑状态机呈现了安全退出与重入、`acks=all` 的两个失败窗口和 epoch 栅栏；同时，其基于 HW 的 promotion 不能被误认为 Kafka 的 leader-epoch 协调。副本权威明确后，下一章上移一层：consumer 也必须就分区归属以及哪个 generation 可以提交达成一致。
