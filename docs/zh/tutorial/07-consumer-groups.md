# 第 7 章 · 消费组

分区为记录提供全序，但应用通常需要多个 worker。消费组把一组独立 consumer 变成一个逻辑订阅者：在稳定 generation 内，组中的每个已订阅分区最多有一个 owner。MiniKafka 保留这一核心不变量，同时把 coordinator 缩小到一次即可读完。

## 学习目标

学完本章，你将能够：

- 追踪消费组从 join、rebalance、稳定运行、过期到 leave 的过程；
- 计算 MiniKafka 的确定性 round-robin 分配；
- 解释 generation 与 ownership 检查如何阻止陈旧 offset commit；
- 区分当前位置与已提交的 next offset；以及
- 准确说明该 coordinator 如何简化 Kafka rebalance 协议。

## Coordinator 持有成员关系真相

主要状态模型在 `src/minikafka/consumer/group.py`。`Group` 包含 generation 编号、`GroupState`、成员表和权威 assignment。每个 `GroupMember` 保存订阅与 session deadline。consumer 对象持有本地副本，但 coordinator 才是权威。

`GroupCoordinator.join` 校验 ID 与 topic 名，创建或替换 member，把 deadline 设为 `now + session_timeout_ms`，再调用 `GroupCoordinator._rebalance`。该方法显式完成以下转换：

1. 设置 `PREPARING_REBALANCE`；
2. 递增 generation；
3. 计算 assignment；若没有成员则进入 `EMPTY`；
4. 设置 `STABLE`。

这一序列同步完成。正常实现中，不会有其他 coroutine 观察到长时间存在的 preparing 阶段。这个状态用来讲清 coordinator 契约，并让 `assignment` 与 `commit` 在组不稳定时拒绝工作。

用同一个 member ID 再次 join 会替换它的订阅，并创建新 generation。`GroupCoordinator.leave` 删除已知成员后走同一 rebalance 路径；对未知成员的 leave 是幂等的。这些选择分别可见于 `src/minikafka/consumer/group.py` 的 `GroupCoordinator.join` 与 `GroupCoordinator.leave`。

## Assignment 确定且理解订阅

`src/minikafka/consumer/assignor.py` 的 `round_robin_assign` 先对 member ID、topic 名和 partition 编号排序。对每个 topic，它建立订阅该 topic 的已排序成员列表，然后用一个共享 cursor 轮转候选成员。若 `a`、`b` 都订阅四个分区，结果为 `a -> (0, 2)`、`b -> (1, 3)`。

在教学系统中，排序不只是美观。它消除了字典顺序和到达顺序给测试带来的偶然性。同一份成员/订阅映射与 topic metadata 必然产生同一结果。

cursor 在 topic 之间共享，而不是每个 topic 重新归零。因此分配在完整有序分区流上大致轮转，但这只是一个策略。MiniKafka 没有 range、sticky 或 cooperative-sticky assignor，也没有 assignor 协商插件面。

`src/minikafka/consumer/consumer.py` 的 `Consumer.subscribe` 调用 `join`，并通过 `Consumer._apply_join` 复制返回的 generation 与 assignment。该 helper 还会丢弃已不再拥有的分区位置。后续组变化**不会**推送进旧 consumer 对象；对象必须调用 `Consumer.refresh_assignment` 获取 coordinator 新视图。

这个间隙是有意且有教学价值的。第二个 member 刚 join 时，第一个对象的本地 assignment 可能与第二个的新 assignment 重叠。本地认知已陈旧，但 coordinator 权威没有。数据面调用者应当 refresh，而 coordinator 的 commit 栅栏会阻止陈旧 owner 发布进度。

## Generation 栅栏保护已提交进度

generation 是一次完整 assignment 的权威令牌。每次改变成员关系的 rebalance 都会递增它。`src/minikafka/consumer/group.py` 的 `GroupCoordinator._validate_generation` 对任何不一致抛出 `IllegalGeneration`。

这项检查出现在两条重要路径：

- `GroupCoordinator.heartbeat` 先校验 generation 与成员身份，再延长 deadline。
- `GroupCoordinator.commit` 要求状态为 `STABLE`，校验 generation 和 member 是否存在，再检查请求中的每个分区是否属于该 member。违反归属会抛出 `NotPartitionOwner`。

只有全部检查通过，`GroupCoordinator.commit` 才调用 `OffsetStore.commit`。因此，即使出现前述本地 assignment 重叠，旧 worker 也无法覆盖新 owner 的进度。

`src/minikafka/consumer/consumer.py` 的 `Consumer.poll` 维护本地**位置**：该 consumer 对象下次要拉取的 offset。poll 会推进它；`Consumer.commit` 把这些位置发布为组的已提交 next offset。二者刻意分离。处理完成但 commit 前崩溃会重放记录；先 commit、后处理则可能跳过记录。

对于直接 assign 的 consumer，`Consumer.commit` 不经过 coordinator generation，直接写 offset store，因为不存在基于 subscription 的组 ownership 需要栅栏。对于 subscribed consumer，它总是经由 `GroupCoordinator.commit`。

`src/minikafka/consumer/offsets.py` 的 `OffsetStore.commit_many_sync` 使用临时文件、`fsync`、`os.replace` 和父目录 `fsync` 持久化完整 offset map。持久对象是本地 JSON 文件，而不是 MiniKafka topic。

## 心跳是租约，不是工作确认

`GroupCoordinator.heartbeat` 根据注入时钟设置新 deadline。`GroupCoordinator.expire_members` 扫描所有组，删除满足 `now > deadline_ms` 的成员，并只对确实丢失成员的组 rebalance。注意严格的 `>`：在 deadline 恰好到达时，成员还没有过期。

公开推进入口是 `src/minikafka/core/cluster.py` 的 `BrokerCluster.expire_group_members`。没有后台任务自动运行。测试使用 `src/minikafka/clock.py` 的 `ManualClock` 确定性推进时间并显式调用过期。

heartbeat 只表达“该成员 session 仍活着”。它不会 commit 记录，不会证明处理成功，也不会刷新另一个 consumer 对象的本地 assignment。把活性、ownership、本地 position 和 committed progress 分开，是本章最重要的心智模型。

## 与真实 Kafka 对照

精确边界列在[消费组协调映射](../kafka-mapping.md)中，可执行论断由[行为矩阵](../behavior-matrix.md)索引。

MiniKafka 保留了 stable-generation 单一归属、heartbeat/commit 的 generation 检查以及 commit 的 ownership 检查。这些都是有价值的 Kafka 不变量，但外围编排被刻意缩减：

- Kafka classic protocol 分离 JoinGroup 与 SyncGroup，并选择 group leader 参与分配；MiniKafka 在 coordinator 中同步 join 与 assign。
- Kafka 有 rebalance timeout 和 revoke/assignment callback；MiniKafka 没有 revoke barrier，本地 assignment 仅在显式 refresh 时变化。
- Kafka 支持 eager 与 cooperative 协议，包括 KIP-429；MiniKafka 只提供一个 eager round-robin 结果。
- 真实客户端在后台 heartbeat，并协调 poll 限制；MiniKafka 用注入时钟与显式 expiry，不建模 KIP-62 的后台活性拆分。
- Kafka 把组 metadata 和 offset 存进经过复制与压实的 `__consumer_offsets` topic；MiniKafka 使用一个原子 JSON 文件，所以 offset 持久性没有走本项目的复制或压实。
- 错误分类被合并：未知 group 当前会变成 `UnknownMember`，而 Kafka 会区分 group 与 member 错误。

因此，不要把 MiniKafka coordinator 描述为 wire-compatible 或生产可扩展。准确说法是：它是 ownership generation 与 commit fencing 的确定性模型。

member identity 与 generation identity 同样需要谨慎。MiniKafka 在 `src/minikafka/core/cluster.py` 的 `BrokerCluster.consumer` 中分配进程本地名称；Kafka client 则可能使用生成身份或 static membership identity。稳定 member identity 可以减少生产消费组中不必要的移动，但永远不能取代 generation 校验：coordinator 仍须证明该 member 参与当前 assignment。换句话说，“你是谁？”和“你正在哪个 assignment 版本下行动？”是两个不同问题。

## 动手实验：观察 rebalance

仓库内置了一个使用临时存储、无需 socket 的实验：

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run python -m minikafka.labs.rebalance
```

实测输出：

```text
1. First member joins and initially owns every partition.
   member 1: orders-0, orders-1, orders-2, orders-3
2. Second member joins, triggering a new generation.
   member 2: orders-1, orders-3
   member 1 local view before refresh: orders-0, orders-1, orders-2, orders-3
   MiniKafka deliberately has no revoke barrier: the old local assignment remains until refresh_assignment().
3. Member 1 refreshes and observes the stable assignment.
   member 1: orders-0, orders-2
   member 2: orders-1, orders-3
   overlap after refresh: False
4. Member 2 leaves; the remaining member owns all partitions.
   member 1: orders-0, orders-1, orders-2, orders-3
   Real Kafka coordinates JoinGroup/SyncGroup and revocation; this mini coordinator completes rebalance synchronously.
```

真正有意思的是步骤 2 和 3 之间。member 1 的 Python 对象仍列出全部四个分区，但 coordinator generation 已把两个分区分给 member 2。member 1 的陈旧 commit 会在触及 offset store 之前失败。

还可以直接验证两个栅栏分支：

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run pytest -q \
  tests/consumer/test_generation_fencing.py
```

实测输出：

```text
..                                                                       [100%]
2 passed in 0.04s
```

第一个测试在 rebalance 后拒绝旧 generation；第二个拒绝当前 generation 的 member 提交属于其他 member 的分区。

## 练习

1. **理解题：** consumer 已 poll 到 offset 9，因此 position 是 10，但 committed offset 是 6。替代 consumer 从哪里开始？为什么？

    ??? note "参考答案"

        从 committed offset 6 开始。position 属于仍存活的 consumer 对象；已提交 next offset 才是持久化的组进度。offset 6 到 9 可能重放，形成 at-least-once 行为。

2. **动手题：** 把 rebalance lab 复制到 `/tmp/rebalance_lab.py`。在 `first.refresh_assignment()` 前，通过 poll 初始化 position 后调用 `await first.commit()`。验收：捕获并打印 `ILLEGAL_GENERATION`；refresh 后 commit 成功。不要修改 `src/`。

    ??? note "参考答案"

        第一个对象保留首次 join 返回的 generation；第二次 join 递增 coordinator generation，所以 `GroupCoordinator._validate_generation` 拒绝旧令牌。refresh 复制当前 generation 和 assignment，之后受范围限制的 commit 合法。

3. **动手题：** 用 `ManualClock`、100 ms session timeout 和两个 member 编写 `/tmp` 脚本。先推进到恰好 100 ms 并 expire，再推进 1 ms 再 expire。验收：第一次结果为空，第二次列出两个过期 member。

    ??? note "参考答案"

        clock 为零时 join，deadline 为 100。代码检查 `now > deadline_ms` 而非 `>=`，所以 100 时返回 `()`；101 时两者被删除，rebalance 令组变空。

4. **理解题：** generation 已合法，为什么仍需 ownership 检查？

    ??? note "参考答案"

        generation 标识 assignment 版本，不标识某个 member 的具体分区。同一当前 generation 由多个 member 共享。没有 ownership 子集检查，任一 member 都能提交其他 member 的分区进度。

## 小结

消费组正确性依赖四类相互分离的状态：coordinator membership、受 generation 约束的 assignment、consumer 的本地 fetch position，以及持久化 committed progress。MiniKafka 合并了 Kafka 的 rebalance 编排，但保留阻止陈旧 owner 发布 offset 的栅栏检查。下一章将这些 offset 与输出记录放进事务，在 HW 之外再增加一层可见性边界。
