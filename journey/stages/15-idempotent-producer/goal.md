# Stage 15 · Idempotent producer retries / 幂等 Producer 重试

<!-- journey: chapter=4 tests_added=2 -->

## English

### Goal

Build idempotent producer retries and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/core/cluster.py`
- `src/minikafka/errors.py`
- `src/minikafka/producer/producer.py`
- `src/minikafka/producer/state.py`
- `src/minikafka/replication/replica_set.py`
- `tests/producer/test_idempotence.py`
- `tests/reliability/test_producer_state_restart.py`

### The problem at this point

A lost response makes retry necessary, but a blind retry duplicates a record that may already be durable.

### Test contract

#### See the failure first

Tests resend the exact sequence, create a sequence gap, restart producer state, and start a second instance with the same transactional identity.

<!-- journey-file: tests/producer/test_idempotence.py -->
<!-- journey-file: tests/reliability/test_producer_state_restart.py -->
#### Idempotent producer retries test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Tests resend the exact sequence, create a sequence gap, restart producer state, and start a second instance with the same transactional identity.

##### Key test statement

```python
assert second == first
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

Producer ID and epoch identify authority; per-partition sequence numbers identify order. An exact retry returns the original offsets, while gaps and old epochs are fenced.

### Why this mechanism is necessary

A lost response makes retry necessary, but a blind retry duplicates a record that may already be durable. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Before append, the state manager compares epoch and sequence with durable per-partition state; after append, it records the batch result for replay across restart.

### Mechanism blocks

<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/errors.py -->
<!-- journey-file: src/minikafka/producer/producer.py -->
<!-- journey-file: src/minikafka/producer/state.py -->
<!-- journey-file: src/minikafka/replication/replica_set.py -->
#### Idempotent producer retries mechanism

##### What it is and why it appears

Producer ID and epoch identify authority; per-partition sequence numbers identify order. An exact retry returns the original offsets, while gaps and old epochs are fenced.

##### Runtime role

Before append, the state manager compares epoch and sequence with durable per-partition state; after append, it records the batch result for replay across restart.

##### Statement understanding

Only the next sequence may append, while the immediately repeated sequence may reuse its stored result; these are different branches, not one loose inequality.



### Verification evidence

Run `uv run pytest -q $(cat journey/stages/15-idempotent-producer/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Only the next sequence may append, while the immediately repeated sequence may reuse its stored result; these are different branches, not one loose inequality.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 4](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/04-producer.md)

## 中文

### 目标

实现幂等 Producer 重试，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/core/cluster.py`
- `src/minikafka/errors.py`
- `src/minikafka/producer/producer.py`
- `src/minikafka/producer/state.py`
- `src/minikafka/replication/replica_set.py`
- `tests/producer/test_idempotence.py`
- `tests/reliability/test_producer_state_restart.py`

### 当前遇到的问题

响应丢失会迫使重试，但盲目重试会复制可能已经持久化的 Record。

### 测试契约

#### 先看会坏在哪里

测试重发相同 Sequence、制造 Sequence Gap、重启 Producer State，并用同一身份启动第二实例。

<!-- journey-file: tests/producer/test_idempotence.py -->
<!-- journey-file: tests/reliability/test_producer_state_restart.py -->
#### 幂等 Producer 重试测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

测试重发相同 Sequence、制造 Sequence Gap、重启 Producer State，并用同一身份启动第二实例。

##### 关键测试语句

```python
assert second == first
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Producer ID 与 Epoch 标识权威；每 Partition Sequence 标识顺序。精确重试返回原 Offset，Gap 与旧 Epoch 被 Fencing。

### 为什么需要这个机制

响应丢失会迫使重试，但盲目重试会复制可能已经持久化的 Record。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Append 前 State Manager 用持久的分区状态比较 Epoch 与 Sequence；Append 后记录 Batch Result，使重启后仍可复用。

### 机制板块

<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/errors.py -->
<!-- journey-file: src/minikafka/producer/producer.py -->
<!-- journey-file: src/minikafka/producer/state.py -->
<!-- journey-file: src/minikafka/replication/replica_set.py -->
#### 幂等 Producer 重试机制

##### 是什么，为什么现在需要

Producer ID 与 Epoch 标识权威；每 Partition Sequence 标识顺序。精确重试返回原 Offset，Gap 与旧 Epoch 被 Fencing。

##### 在运行时做什么

Append 前 State Manager 用持久的分区状态比较 Epoch 与 Sequence；Append 后记录 Batch Result，使重启后仍可复用。

##### 关键语句理解

只有 Next Sequence 可以追加，而刚完成的重复 Sequence 可以复用保存结果；这是两个分支，不是一个宽松不等式。



### 验证证据

运行 `uv run pytest -q $(cat journey/stages/15-idempotent-producer/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

只有 Next Sequence 可以追加，而刚完成的重复 Sequence 可以复用保存结果；这是两个分支，不是一个宽松不等式。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 4 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/04-producer.md)
