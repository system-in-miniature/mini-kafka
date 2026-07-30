# 第 8 章 · 事务

复制回答“哪个日志前缀足够持久，可以读取？”事务又增加一个问题：“哪些记录属于已经完成的工作单元？”一个事务可以向多个分区追加输出，并暂存已消费 offset。使用 `read_committed` 的读者既不能看见未完成事务，也不能看见已中止事务；成功 commit 则必须一起发布输出和输入进度。

## 学习目标

学完本章，你将能够：

- 追踪 MiniKafka 的事务状态与 journal 记录；
- 解释 PREPARE 为什么是恢复过程中的持久决策边界；
- 区分 HW 与 last stable offset（LSO）；
- 说明 commit/abort control marker 如何影响 `read_committed`；以及
- 解释为什么 `acks=all` 对 Kafka 事务必要但不充分。

## 先有持久状态机，再有可见性规则

`src/minikafka/transaction/model.py` 的 `TransactionState` 定义五个状态：`ONGOING`、`PREPARE_COMMIT`、`COMPLETE_COMMIT`、`PREPARE_ABORT` 和 `COMPLETE_ABORT`。`TransactionData` 把 transaction ID 与触及的分区、各分区第一条写入 offset、暂存的 group offset 关联起来。

这些字段回答不同的恢复问题。`partitions` 告诉 commit/abort 应向哪里写 marker；`first_offsets` 告诉读者不稳定事务从哪里开始；`staged_offsets` 保存不能过早可见的 consumer progress。

`src/minikafka/transaction/journal.py` 的 `TransactionJournal.append` 把完整当前状态序列化为一个 JSON payload，加上 CRC32 前缀，追加换行，flush 并调用 `fsync`。journal 是状态快照序列，不是原地修改的数据库。`TransactionJournal.recover` 扫描 CRC 合法的 frame，并为每个 transaction ID 保留最后状态。最终一行若不完整、格式错误或 CRC 错误，文件会被截断到最后一个合法边界。

这和 append-only log 的尾部修复思想相同：崩溃可能留下不完整的最后一次写入，但不能把任意更早的损坏当成合法状态。该 journal 是本地单进程文件，没有经过 MiniKafka 分区复制。这里有一个刻意保留的局限：恢复在**首个坏行**处停止，并整体截断其后的全部内容；即使后续行本身格式与 CRC 都合法，也不会越过中间损坏重新同步。这是尾部修复，不是中段损坏后的 resynchronization。

`src/minikafka/transaction/manager.py` 的 `TransactionManager.__init__` 恢复 journal 后立即调用 `TransactionManager._finish_recovery`。PREPARE 状态之所以重要，是因为它们记录了持久决定：

- `PREPARE_COMMIT` 同步发布 staged offset，并变成 `COMPLETE_COMMIT`。
- `PREPARE_ABORT` 清空 staged offset，并变成 `COMPLETE_ABORT`。

恢复方法不会补写缺失 control marker。正常 commit 在 marker 之前写 PREPARE，因此该间隔内崩溃可能导致 journal 判定事务已 commit，却并非所有 marker 都进入日志。这是刻意的简化，也是与 Kafka coordinator protocol 对照时不可忽略的边界。

## 事务数据已追加，但最初不稳定

`TransactionManager.begin` 拒绝复用活跃 ID，创建 `ONGOING` 记录并写 journal。`Transaction.send` 委托给 `TransactionManager.send`；后者先要求状态为 `ONGOING`，选择分区，并构造携带 `transactional_id` 的 `RecordBatch`。

该 batch 通过 `BrokerCluster.append_batch` 以 `AckMode.ALL` 追加。成功后，manager 记录分区和第一条 base offset，再写入新状态。使用 `acks=all` 保证 captured ISR 仍有副本缺失数据时，该事务数据步骤不会报告完成。

这条路径不使用 producer PID、epoch 或 sequence。transaction ID 只是 journal/visibility 身份。这个区别极其关键：持久复制不等于幂等重试，也不等于 zombie producer 栅栏。

`Transaction.send_offsets` 不写 `OffsetStore`，只更新 `staged_offsets` 并写 journal。commit 之前，新 consumer 仍读到旧 committed offset。

## PREPARE、marker 与完成

`src/minikafka/transaction/manager.py` 的 `TransactionManager.commit` 按以下顺序执行：

1. 要求 `ONGOING`；
2. 设置 `PREPARE_COMMIT` 并写 journal；
3. 向每个已触及分区以 `acks=all` 追加 COMMIT control marker；
4. 通过 `OffsetStore.commit_many` 发布所有 staged offset；
5. 设置并记录 `COMPLETE_COMMIT`。

`TransactionManager.abort` 以 `PREPARE_ABORT`、ABORT marker、清空 staged offset 和 `COMPLETE_ABORT` 镜像这一流程。

control batch 来自 `src/minikafka/core/batch.py` 的 `RecordBatch.control_marker`。它不包含用户记录，必须带 transaction ID，消耗一个逻辑 offset，并携带 `ControlType.COMMIT` 或 `ControlType.ABORT`。consumer 不返回这些 control record，但它们在日志中的位置封闭了事务。

数据和 marker 都使用 `acks=all`。`tests/transaction/test_visibility.py` 和 `tests/transaction/test_abort.py` 中的聚焦测试在双副本分区上把每项操作启动为 task，并证明在 `BrokerCluster.replicate_all_once` 推进 follower 之前操作保持 pending。这避免了“事务数据已复制，但决定 marker 只有 leader 本地副本”的危险半契约。

这里的 output-plus-offset 原子性局限于本地 coordinator。代码先写完全部 output marker，再发布 staged offset；abort 时则丢弃 staged offset。它不提供跨独立 coordinator 或 cluster 的分布式原子 commit。

## HW 与 LSO 回答不同问题

HW 是按当前 ISR 边界充分复制的排他终点，其中可能包含开放事务的数据。`read_uncommitted` 读到 HW，所以可以观察这些数据。

`src/minikafka/transaction/manager.py` 的 `TransactionManager.last_stable_offset` 在状态为 `ONGOING`、`PREPARE_COMMIT` 或 `PREPARE_ABORT` 的事务中寻找最小 first offset；若没有不稳定事务，则默认为 HW。这个值就是 LSO。它阻止 `read_committed` consumer 越过未完成事务读取后续记录，因为前一事务的结果尚未确定。

`src/minikafka/core/cluster.py` 的 `BrokerCluster.fetch` 对 `IsolationLevel.READ_COMMITTED` 把 LSO 用作 `end_offset`，然后跳过 control batch，并跳过所有没有被 `TransactionManager.is_committed` 判定为 `COMPLETE_COMMIT` 的事务 batch。中止记录仍物理存在于日志中，但不会在该 isolation level 返回。

必须同时有两类过滤：

- LSO 阻止第一项不稳定事务及其后的全部记录；
- committed-state 过滤隐藏已经低于新 LSO 的 aborted batch。

`src/minikafka/replication/replica_set.py` 的 `PartitionReplicaSet.fetch` 对其异步白盒读取路径包含相同的 LSO 与状态过滤概念。公开的 cluster fetch 仍是标准 Direct API。

## 与真实 Kafka 对照

请用仓库的[事务与可见性映射](../kafka-mapping.md)和[行为矩阵](../behavior-matrix.md)划定迁移边界。

MiniKafka 保留了 KIP-98 的若干核心思想：transactional record batch、commit/abort control marker、LSO 限制的 `read_committed`，以及只在 commit 时发布的 staged input offset。数据与 marker 都用 `acks=all` 复制。

缺少的机制同样重要：

- Kafka transaction coordinator 把状态持久化到经过复制的 `__transaction_state` 内部 topic；MiniKafka 只有一个本地 CRC journal。
- Kafka 事务建立在幂等 producer 之上。`transactional.id` 关联 producer epoch，使新实例能 fence zombie；MiniKafka transaction ID 独立于 `ProducerStateManager`，事务 send 没有 PID/epoch/sequence。
- Kafka 协调 marker 完成、重试、超时和 coordinator migration；MiniKafka 在启动时本地完成 PREPARE，且恢复过程不会修复缺失 marker。
- Kafka 的 `sendOffsetsToTransaction` 把 offset 与 group metadata、coordinator protocol 关联；MiniKafka 暂存 map，最后发布到本地 JSON `OffsetStore`。

所以，“MiniKafka 使用 `acks=all`”证明的是复制属性，而非单独证明 exactly-once processing。Exactly once 还需要幂等生产、zombie fencing、read-committed 消费和原子 offset 发布。

评估恢复时，应检查 commit 五个步骤之间的每个切点，而不只看整个方法之前与之后。逐一询问：哪个决定已经持久化、哪些 marker 已存在、offset 是否已经发布、重启 coordinator 将采取什么动作。这样的切点分析既说明 PREPARE 为什么必要，也揭示 MiniKafka 本地完成规则为什么比 Kafka 分布式事务恢复更窄。

## 动手实验：观察可见性边界

在仓库根目录运行以下无需 socket、使用临时目录的实验：

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run python - <<'PY'
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition
from minikafka.clock import ManualClock
from minikafka.replication.model import IsolationLevel

async def main():
    with TemporaryDirectory() as d:
        async with BrokerCluster.open(
            MiniKafkaConfig(data_dir=Path(d)), clock=ManualClock()
        ) as cluster:
            await cluster.create_topic("out", 1, 1)
            tp = TopicPartition("out", 0)
            tx = await cluster.transactions.begin("tx-1")
            await tx.send("out", value=b"pending")
            print("before marker, read_uncommitted:",
                  [r.value for r in cluster.fetch(tp, 0, 10)])
            print("before marker, read_committed:",
                  [r.value for r in cluster.fetch(
                      tp, 0, 10, IsolationLevel.READ_COMMITTED)])
            await tx.commit()
            print("after commit marker:",
                  [r.value for r in cluster.fetch(
                      tp, 0, 10, IsolationLevel.READ_COMMITTED)])
            tx2 = await cluster.transactions.begin("tx-2")
            await tx2.send("out", value=b"discard-me")
            await tx2.abort()
            print("after abort marker:",
                  [r.value for r in cluster.fetch(
                      tp, 0, 10, IsolationLevel.READ_COMMITTED)])

asyncio.run(main())
PY
```

实测输出：

```text
before marker, read_uncommitted: [b'pending']
before marker, read_committed: []
after commit marker: [b'pending']
after abort marker: [b'pending']
```

被 abort 的值没有从磁盘消失，而是被 isolation 过滤。查看 `cluster.leader_log(tp).all_batches()` 会看到数据与两个 control marker，但面向 consumer 的结果仍只有一个 committed value。

对双副本 `acks=all` 契约运行：

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run pytest -q \
  tests/transaction/test_visibility.py tests/transaction/test_abort.py
```

实测输出：

```text
......                                                                   [100%]
6 passed in 0.48s
```

## 练习

1. **理解题：** 为什么 `read_committed` 不能简单跳过 open transaction，返回更晚的非事务记录？

    ??? note "参考答案"

        这样会暴露越过未决顺序边界的记录。open transaction 之后可能 commit；若先返回后续 offset，就无法提供稳定前缀视图。LSO 在第一项不稳定事务处停止；结果确定后，扫描才能继续并过滤 aborted batch。

2. **动手题：** 在 `/tmp/tx_offsets.py` 中扩展实验，加入 input `TopicPartition` 和 `await tx.send_offsets("workers", {input_tp: 7})`，打印 commit 前后 committed offset。验收：commit 前为 `None`，commit 后为 `7`。不要修改 `src/`。

    ??? note "参考答案"

        在 `await tx.commit()` 两侧调用 `await cluster.offsets.get("workers", input_tp)`。`send_offsets` 只改变已写 journal 的 `staged_offsets`；`TransactionManager.commit` 在全部 commit marker 成功后才发布 map。

3. **动手恢复题：** 单独运行 `tests/reliability/test_transaction_restart.py`，然后解释它证明的三种情况。验收：`3 passed`，解释需提到 committed visibility、PREPARE_COMMIT offset completion 和坏尾截断。

    ??? note "参考答案"

        运行 `uv run pytest -q tests/reliability/test_transaction_restart.py`。测试证明 completed transaction 重开后仍可见；恢复会把 journal 中的 PREPARE_COMMIT 转成 committed offset 与 COMPLETE_COMMIT；不完整的最后 journal frame 会被截断。它们不证明 replicated coordinator failover。

4. **理解题：** 要 fence 两个使用相同 transaction name 的进程，还需要什么身份？

    ??? note "参考答案"

        需要 coordinator 分配的 producer ID，以及绑定到 transactional ID 且单调增加的 producer epoch。每个事务写都必须携带 epoch，使新的初始化能 fence 旧进程。MiniKafka 事务路径目前没有连接这份状态。

## 小结

MiniKafka 事务在复制日志之上叠加了 journaled decision state 和 LSO 可见性边界。PREPARE 记录恢复决定，marker 决定分区可见性，staged offset 只在 commit 后发布。这个模型呈现了组合关系；本地 journal 与缺失的事务 producer fencing 则划出了真实 Kafka 边界。下一章把这些机制放进失败实验，组装 at-most-once、at-least-once 与 exactly-once 背后的条件。
