# Stage 07 · Partitioned producer batching / 分区化 Producer Batching

<!-- journey: chapter=4 tests_added=3 -->

## English

### Goal

Build partitioned producer batching and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/core/cluster.py`
- `src/minikafka/errors.py`
- `src/minikafka/producer/__init__.py`
- `src/minikafka/producer/accumulator.py`
- `src/minikafka/producer/partitioner.py`
- `src/minikafka/producer/producer.py`
- `tests/producer/test_batching.py`
- `tests/producer/test_ordering.py`
- `tests/unit/test_partitioner.py`

### The problem at this point

Per-record appends waste batching opportunities, while unbounded buffering or unstable partition choice breaks latency and ordering expectations.

### Test contract

#### See the failure first

Tests force both linger and batch-size flush paths, fill the bounded buffer, and mix keyed, keyless, and explicit partition sends.

<!-- journey-file: tests/producer/test_batching.py -->
<!-- journey-file: tests/producer/test_ordering.py -->
<!-- journey-file: tests/unit/test_partitioner.py -->
#### Partitioned producer batching test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Tests force both linger and batch-size flush paths, fill the bounded buffer, and mix keyed, keyless, and explicit partition sends.

##### Key test statement

```python
assert not first.done()
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

The Partitioner chooses a partition; the Accumulator owns pending per-partition batches; linger is a latency bound; batch size is a throughput trigger.

### Why this mechanism is necessary

Per-record appends waste batching opportunities, while unbounded buffering or unstable partition choice breaks latency and ordering expectations. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Send chooses one partition, enqueues a pending record, and schedules flush when size or time closes the batch; acknowledgements resolve record futures in partition order.

### Mechanism blocks

<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/errors.py -->
<!-- journey-file: src/minikafka/producer/accumulator.py -->
<!-- journey-file: src/minikafka/producer/partitioner.py -->
<!-- journey-file: src/minikafka/producer/producer.py -->
#### Partitioned producer batching mechanism

##### What it is and why it appears

The Partitioner chooses a partition; the Accumulator owns pending per-partition batches; linger is a latency bound; batch size is a throughput trigger.

##### Runtime role

Send chooses one partition, enqueues a pending record, and schedules flush when size or time closes the batch; acknowledgements resolve record futures in partition order.

##### Statement understanding

A keyed record hashes stably and an open keyless batch stays sticky, so batching efficiency never silently changes same-partition order.

<!-- journey-file: src/minikafka/producer/__init__.py -->
#### Package and project support

These files only keep the package boundary, dependencies, and test environment reproducible; they are supporting wiring rather than Kafka mechanism logic.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/07-producer-batching/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

A keyed record hashes stably and an open keyless batch stays sticky, so batching efficiency never silently changes same-partition order.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 4](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/04-producer.md)

## 中文

### 目标

实现分区化 Producer Batching，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/core/cluster.py`
- `src/minikafka/errors.py`
- `src/minikafka/producer/__init__.py`
- `src/minikafka/producer/accumulator.py`
- `src/minikafka/producer/partitioner.py`
- `src/minikafka/producer/producer.py`
- `tests/producer/test_batching.py`
- `tests/producer/test_ordering.py`
- `tests/unit/test_partitioner.py`

### 当前遇到的问题

逐 Record 追加浪费批处理机会；无界缓冲或不稳定分区选择又会破坏延迟与顺序预期。

### 测试契约

#### 先看会坏在哪里

测试分别触发 Linger 与 Batch Size Flush，填满有界缓冲，并混合 Keyed、Keyless 与显式分区发送。

<!-- journey-file: tests/producer/test_batching.py -->
<!-- journey-file: tests/producer/test_ordering.py -->
<!-- journey-file: tests/unit/test_partitioner.py -->
#### 分区化 Producer Batching测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

测试分别触发 Linger 与 Batch Size Flush，填满有界缓冲，并混合 Keyed、Keyless 与显式分区发送。

##### 关键测试语句

```python
assert not first.done()
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Partitioner 选择分区；Accumulator 拥有各分区待发送 Batch；Linger 是延迟边界；Batch Size 是吞吐触发器。

### 为什么需要这个机制

逐 Record 追加浪费批处理机会；无界缓冲或不稳定分区选择又会破坏延迟与顺序预期。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Send 选定一个 Partition、排入 Pending Record，并在大小或时间关闭 Batch 时 Flush；Ack 按分区顺序完成 Record Future。

### 机制板块

<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/errors.py -->
<!-- journey-file: src/minikafka/producer/accumulator.py -->
<!-- journey-file: src/minikafka/producer/partitioner.py -->
<!-- journey-file: src/minikafka/producer/producer.py -->
#### 分区化 Producer Batching机制

##### 是什么，为什么现在需要

Partitioner 选择分区；Accumulator 拥有各分区待发送 Batch；Linger 是延迟边界；Batch Size 是吞吐触发器。

##### 在运行时做什么

Send 选定一个 Partition、排入 Pending Record，并在大小或时间关闭 Batch 时 Flush；Ack 按分区顺序完成 Record Future。

##### 关键语句理解

Keyed Record 稳定哈希，打开的 Keyless Batch 保持 Sticky，因此批处理效率不会悄悄改变同分区顺序。

<!-- journey-file: src/minikafka/producer/__init__.py -->
#### 包与工程支撑

这些文件只负责让本 Stage 的包边界、依赖与测试环境可复现，不把脚手架误讲成 Kafka 机制。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/07-producer-batching/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

Keyed Record 稳定哈希，打开的 Keyless Batch 保持 Sticky，因此批处理效率不会悄悄改变同分区顺序。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 4 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/04-producer.md)
