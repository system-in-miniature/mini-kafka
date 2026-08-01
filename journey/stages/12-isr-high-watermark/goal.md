# Stage 12 · ISR and high watermark / ISR 与 High Watermark

<!-- journey: chapter=5 tests_added=3 -->

## English

### Goal

Build isr and high watermark and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/config.py`
- `src/minikafka/core/cluster.py`
- `src/minikafka/producer/producer.py`
- `src/minikafka/replication/__init__.py`
- `src/minikafka/replication/model.py`
- `src/minikafka/replication/replica.py`
- `src/minikafka/replication/replica_set.py`
- `tests/replication/test_follower_fetch.py`
- `tests/replication/test_high_watermark.py`
- `tests/replication/test_isr.py`

### The problem at this point

Leader-local append is not committed data when followers may lag or fail.

### Test contract

#### See the failure first

Follower-fetch tests move complete batches, ISR tests expire lagging replicas, and visibility tests refuse reads beyond the high watermark.

<!-- journey-file: tests/replication/test_follower_fetch.py -->
<!-- journey-file: tests/replication/test_high_watermark.py -->
<!-- journey-file: tests/replication/test_isr.py -->
#### ISR and high watermark test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Follower-fetch tests move complete batches, ISR tests expire lagging replicas, and visibility tests refuse reads beyond the high watermark.

##### Key test statement

```python
assert replica_set.replicas[2].leo == 2
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

LEO is a replica's next offset. ISR is the current caught-up replica set. High watermark is the minimum LEO across ISR and bounds committed visibility.

### Why this mechanism is necessary

Leader-local append is not committed data when followers may lag or fail. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Followers fetch from their LEO, append complete batches, report progress, and cause membership and HW to refresh; committed reads stop at HW.

### Mechanism blocks

<!-- journey-file: src/minikafka/config.py -->
<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/producer/producer.py -->
<!-- journey-file: src/minikafka/replication/model.py -->
<!-- journey-file: src/minikafka/replication/replica.py -->
<!-- journey-file: src/minikafka/replication/replica_set.py -->
#### ISR and high watermark mechanism

##### What it is and why it appears

LEO is a replica's next offset. ISR is the current caught-up replica set. High watermark is the minimum LEO across ISR and bounds committed visibility.

##### Runtime role

Followers fetch from their LEO, append complete batches, report progress, and cause membership and HW to refresh; committed reads stop at HW.

##### Statement understanding

Taking the minimum ISR LEO proves every current in-sync replica contains the visible prefix; using the leader LEO would expose unreplicated data.

<!-- journey-file: src/minikafka/replication/__init__.py -->
#### Package and project support

These files only keep the package boundary, dependencies, and test environment reproducible; they are supporting wiring rather than Kafka mechanism logic.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/12-isr-high-watermark/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Taking the minimum ISR LEO proves every current in-sync replica contains the visible prefix; using the leader LEO would expose unreplicated data.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 5](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/05-replication-basics.md)

## 中文

### 目标

实现ISR 与 High Watermark，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/config.py`
- `src/minikafka/core/cluster.py`
- `src/minikafka/producer/producer.py`
- `src/minikafka/replication/__init__.py`
- `src/minikafka/replication/model.py`
- `src/minikafka/replication/replica.py`
- `src/minikafka/replication/replica_set.py`
- `tests/replication/test_follower_fetch.py`
- `tests/replication/test_high_watermark.py`
- `tests/replication/test_isr.py`

### 当前遇到的问题

当 Follower 可能延迟或失败时，Leader 本地 Append 不等于已提交数据。

### 测试契约

#### 先看会坏在哪里

Follower Fetch 测试移动完整 Batch，ISR 测试让落后 Replica 过期，可见性测试拒绝读取 High Watermark 之后的数据。

<!-- journey-file: tests/replication/test_follower_fetch.py -->
<!-- journey-file: tests/replication/test_high_watermark.py -->
<!-- journey-file: tests/replication/test_isr.py -->
#### ISR 与 High Watermark测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

Follower Fetch 测试移动完整 Batch，ISR 测试让落后 Replica 过期，可见性测试拒绝读取 High Watermark 之后的数据。

##### 关键测试语句

```python
assert replica_set.replicas[2].leo == 2
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

LEO 是 Replica 的 Next Offset；ISR 是当前追上的 Replica 集合；High Watermark 是 ISR 中最小 LEO，限制已提交可见性。

### 为什么需要这个机制

当 Follower 可能延迟或失败时，Leader 本地 Append 不等于已提交数据。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Follower 从自身 LEO Fetch、追加完整 Batch、报告进度，并触发 Membership 与 HW 刷新；Committed Read 停在 HW。

### 机制板块

<!-- journey-file: src/minikafka/config.py -->
<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/producer/producer.py -->
<!-- journey-file: src/minikafka/replication/model.py -->
<!-- journey-file: src/minikafka/replication/replica.py -->
<!-- journey-file: src/minikafka/replication/replica_set.py -->
#### ISR 与 High Watermark机制

##### 是什么，为什么现在需要

LEO 是 Replica 的 Next Offset；ISR 是当前追上的 Replica 集合；High Watermark 是 ISR 中最小 LEO，限制已提交可见性。

##### 在运行时做什么

Follower 从自身 LEO Fetch、追加完整 Batch、报告进度，并触发 Membership 与 HW 刷新；Committed Read 停在 HW。

##### 关键语句理解

取 ISR 最小 LEO 证明每个当前同步副本都拥有可见前缀；使用 Leader LEO 会暴露未复制数据。

<!-- journey-file: src/minikafka/replication/__init__.py -->
#### 包与工程支撑

这些文件只负责让本 Stage 的包边界、依赖与测试环境可复现，不把脚手架误讲成 Kafka 机制。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/12-isr-high-watermark/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

取 ISR 最小 LEO 证明每个当前同步副本都拥有可见前缀；使用 Leader LEO 会暴露未复制数据。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 5 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/05-replication-basics.md)
