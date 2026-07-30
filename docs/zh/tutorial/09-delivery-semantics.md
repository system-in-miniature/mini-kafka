# 第 9 章 · 投递语义实验课

“At most once”“at least once”“exactly once”并不是附着在队列上的开关。它们是关于 acknowledgement、重试、处理副作用、offset 发布、复制与故障时机的端到端陈述。本实验章用 MiniKafka 让每条边界都能被观察。

## 学习目标

学完本章，你将能够：

- 重现 `acks=1` 已确认写在故障转移后的丢失；
- 解释 commit 顺序如何导致重复处理或跳过处理；
- 追踪基于 PID、epoch 与 sequence 的 producer 去重；
- 列出 Kafka exactly-once processing 组合的机制；以及
- 把每项投递声明限定到明确的故障模型与可观察边界。

## Acknowledgement 不等于 commitment

`src/minikafka/replication/replica_set.py` 的 `PartitionReplicaSet.append` 对 `AckMode.LEADER`（`acks=1`）在 leader 追加后立即返回。follower LEO 可能仍较小，HW 也可能仍指向新记录之前。producer 已得到 offset，但普通 consumer 还无法读取该记录。

如果 leader 在这个窗口失败，`PartitionReplicaSet.promote` 选择 ISR 候选并把它截断到旧 HW。leader-only tail 不存在于候选中，会从新的权威历史消失。`src/minikafka/labs/leader_failure.py` 的 `main` 无需非确定性 sleep 就构造了这一调度。

这就是 Kafka `acks=1` 的持久性风险：仅有 leader acknowledgement 不证明完成复制。MiniKafka 的手动 promotion 算法不是 Kafka election protocol，所以能迁移的是风险，而不是精确故障转移实现。

`acks=all` 配合合适的 `min.insync.replicas`，通过等待 captured ISR 并拒绝不充分 ISR，关闭这个特定窗口。但它不能单独解决歧义重试。响应可能在追加之后丢失，MiniKafka 也可能抛出 `NotEnoughReplicasAfterAppend`。producer 仍需一种重试但不新建记录的方法。

## 幂等把重试转换为原结果

MiniKafka 在 `src/minikafka/producer/state.py` 的 `ProducerStateManager` 中建模 Kafka 幂等 producer 的 broker 端核心。

幂等 batch 携带：

- producer ID（PID），标识 producer 谱系；
- producer epoch，fence 旧实例；
- base sequence，对分区内 batch 排序。

`ProducerStateManager.validate` 先拒绝比已注册或已记录 epoch 更旧的 epoch，再检查传入 batch 的 sequence 范围是否与最后记录 batch 完全相同。完全相同就返回之前的 `ProduceResult`，不发生第二次追加。否则，next base sequence 必须等于 `previous.last_sequence + 1`，不然抛出 `OutOfOrderSequence`。

`src/minikafka/replication/replica_set.py` 的 `PartitionReplicaSet.append` 在触及 leader log 前调用 `validate`，追加后调用 `record`。构造 replica set 和完成 promotion 时，`ProducerStateManager` 从 leader log 已存在的 batch 重建。因此，restart 不会自然遗忘最后接收的 sequence。

公开构造路径是 `src/minikafka/core/cluster.py` 的 `BrokerCluster.producer`。设置 `idempotent=True` 时，若调用者保留默认 `acks=1`，它会强制改为 `acks=all`；若显式给出不兼容 ack mode，则拒绝。然后通过 `ProducerIdentityStore.allocate` 分配 PID/epoch，并向每个 replica set 注册 epoch。复用 `transactional_name` 会为同一 PID 分配更高 epoch，从而 fence 旧 producer 对象。

限制也是刻意的。每个分区 manager 内的状态只以 PID 为键，duplicate window 只保存最后一个 batch。Kafka 保存最近五个 batch，以匹配允许的 in-flight 窗口。在 MiniKafka 中，batch N+1 已接受后再重试 N，会得到 out-of-order error，而不是命中去重。身份分配来自本地 JSON 文件，不是 broker 协调。

## Consumer 顺序决定重放还是丢失

`src/minikafka/consumer/consumer.py` 的 `Consumer.poll` 推进 consumer 对象本地 position；`Consumer.commit` 单独发布 next offset。没有任何方法能把任意外部副作用与 offset store 原子组合。

以 offset 0 为例：

1. poll 把 position 推到 1；
2. 应用执行副作用；
3. 进程在 commit 1 前崩溃。

替代实例从 committed offset 0 开始并重复副作用。这是典型 at-least-once：不会有意跳过已确认工作，但可能重复。

反转步骤 2 与 3：

1. poll 把 position 推到 1；
2. commit 1；
3. 进程在执行副作用前崩溃。

替代实例从 1 开始，永远不会处理 offset 0。这是典型 at-most-once：用可能丢失换取避免重复。

仓库在 `tests/consumer/test_delivery_semantics.py` 中锁定两种调度：`test_process_before_commit_can_replay_after_crash` 与 `test_commit_before_process_can_skip_after_crash`。即使 broker log 完好，这些仍然是应用投递语义。

## Exactly once 是机制组合

对于 Kafka 风格的 read-process-write，“exactly once”需要一整条机制链：

1. **持久输入：** consumer 读取经过复制的 committed prefix。
2. **幂等输出生产：** 重试复用 PID/epoch/sequence，而不是复制输出。
3. **Zombie fencing：** 更新的 producer epoch 拒绝具有相同事务身份的旧实例写入。
4. **输出与输入 offset 原子化：** produced record 与 next consumed offset 在一个事务中 commit 或 abort。
5. **隔离：** 下游使用 `read_committed`，遵守 LSO 并忽略 aborted batch。
6. **有范围的副作用边界：** 保证覆盖事务内的 Kafka record 与 offset；任意 email、HTTP 或 database write 不在其中，除非这些系统通过自己的幂等或事务协议参与。

MiniKafka 在两个相邻但未完全连接的轨道中实现了这些部件。`src/minikafka/producer/state.py` 为普通 producer 建模 PID/epoch/sequence 幂等；`src/minikafka/transaction/manager.py` 建模 transaction marker、LSO 和 staged offset 原子性，且数据和 marker 使用 `acks=all`。transaction send 路径**不**携带 producer identity 或 epoch。因此，本仓库展示组合要求，却不声称 Kafka-compatible exactly-once processing。

这个否定结论很有价值：它阻止常见等式“事务 API + 成功演示 = exactly once”。只有 input、output、retry、recovery 与 visibility 之间的每条故障边都被封闭，保证才成立。

## 与真实 Kafka 对照

请查阅[producer acknowledgement 映射](../kafka-mapping.md)、[事务映射](../kafka-mapping.md)和[行为矩阵](../behavior-matrix.md)。

`acks=1` 已确认写丢失风险、最后一个 batch 的精确重试去重、epoch fencing、control marker、LSO 和 committed-offset 发布都代表真实 Kafka 思想。主要简化为：

- MiniKafka 显式推进 follower fetch 与 promotion；Kafka 使用网络化 broker 和 controller 管理的选举。
- Direct API 的 `acks=0` 仍会先执行追加，再返回未知 offset；Kafka 不发响应，所以 transport timing 不同。
- MiniKafka 的 `acks=all` waiter 捕获 ISR 并等待每个成员；Kafka 使用完整复制/HW 机制。
- MiniKafka 保留一个 duplicate batch，Kafka 保留五个。
- MiniKafka 事务没有建立在自己的 idempotent producer state 上，也没有 transactional-ID zombie fencing。
- transaction 与 offset coordinator 是本地文件/对象，而非经过复制的 Kafka 内部 topic。

在设计评审中陈述投递保证时，应明确单位（例如“一个 Kafka transaction 内的 records”）、reader isolation、acknowledgement/retry policy，以及不覆盖的外部副作用。

## 实验 1：丢失已确认的 leader-only 写

运行：

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run python -m minikafka.labs.leader_failure
```

实测输出：

```text
1. Producer uses acks=1 (leader acknowledgement only).
   acknowledged offset: 0
   consumer-visible end (HW): 0
   The write is acknowledged but has not reached the follower.
2. Simulate the leader failing before follower replication.
   broker 2 is promoted; its log did not contain offset 0.
   records after failover: 0
3. Result: the acknowledged write was lost.
   Kafka has the same acks=1 risk; use acks=all with an appropriate min.insync.replicas for stronger durability.
```

两个零含义不同。`acknowledged offset: 0` 是分配给记录的 offset；`consumer-visible end (HW): 0` 是排他边界，因此没有记录可见。promotion 后的结果长度证明 tail 已消失。

## 实验 2：证明精确重试去重

运行聚焦 producer 测试：

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run pytest -q \
  tests/producer/test_idempotence.py::test_exact_retry_returns_original_offsets
```

实测输出：

```text
.                                                                        [100%]
1 passed in 0.08s
```

测试创建一个带 sequence 的 batch，等待 `acks=all`，再提交完全相同的 batch。它断言两个 `ProduceResult` 相等且 leader LEO 仍为 1。因此，通过意味着“返回原结果且没有第二次追加”，不只是“没有异常”。

第三项检查是运行 consumer 故障调度：

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run pytest -q \
  tests/consumer/test_delivery_semantics.py
```

实测输出：

```text
..                                                                       [100%]
2 passed in 0.12s
```

## 练习

1. **理解题：** producer 遇到追加后错误，用新 sequence number 重试相同 payload。为什么内容相同不能保证幂等安全？

    ??? note "参考答案"

        两个合法业务事件可能拥有相同 bytes。去重需要请求身份和顺序，而非 payload 相等。重试必须复用相同 PID、epoch 与 sequence，使 broker 能返回原结果。

2. **动手题：** 把 `tests/producer/test_idempotence.py` 的 `sequenced` helper 与 exact-retry 场景复制到 `/tmp/dedup_lab.py`，打印每次 send 后的 LEO，再把 retry sequence 从 0 改为 1。验收：精确重试打印 `1, 1`；sequence 1 是新追加，最终 LEO 为 2。不要修改 `src/`。

    ??? note "参考答案"

        sequence 0 与最后记录 batch 完全匹配，所以 `validate` 返回已保存结果。sequence 1 等于期望 next sequence，代表新记录，因此被追加。复用相同 payload 不影响任一判断。

3. **动手题：** 写一个 scratch consumer 测试：poll 一条记录，把副作用写进 Python list，不 commit，重建 consumer 后再次 poll。验收：list 包含同一值两次，持久 committed offset 仍为 `None`。

    ??? note "参考答案"

        为两个 consumer 对象使用同一 group ID 并直接 assign 同一分区。处理第一次 poll 后不 `commit`，关闭/重建并从 earliest poll。把两次结果都 append 到 list，即可展示重放。这是 at-least-once processing，不是 broker 复制了记录。

4. **理解题：** MiniKafka 当前 transaction API 是否提供端到端 exactly once？

    ??? note "参考答案"

        不提供。它有 marker、LSO/read-committed visibility、原子 staged offset 和 `acks=all` transaction append，但 transaction 路径与 PID/epoch/sequence state 分离，因此缺少幂等事务重试与 transactional-ID zombie fencing。外部副作用也不在保证边界中。

## 小结

投递语义由故障顺序产生。`acks=1` 可能丢失已确认 leader-only tail；先 process 后 commit 可能重放；先 commit 后 process 可能跳过；PID/epoch/sequence 让精确重试幂等；transaction 与 read-committed isolation 原子发布 Kafka 范围内的工作。MiniKafka 同时展示每个部件以及幂等与事务之间尚未连接的部分。最后一章将在该语义内核外增加 transport，并解释为什么这仍不等于 Kafka broker。
