# Stage 13 · Acknowledgement modes / 确认模式

<!-- journey: chapter=6 tests_added=2 -->

## English

### Goal

Build acknowledgement modes and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/errors.py`
- `src/minikafka/producer/producer.py`
- `src/minikafka/replication/replica_set.py`
- `tests/reliability/test_lost_acked_write.py`
- `tests/replication/test_ack_modes.py`

### The problem at this point

One word such as successful is ambiguous unless the producer knows which durability boundary acknowledged the write.

### Test contract

#### See the failure first

The contract contrasts `acks=0`, `acks=1`, and `acks=all`, then shrinks ISR both before and after append to expose two distinct failure windows.

<!-- journey-file: tests/reliability/test_lost_acked_write.py -->
<!-- journey-file: tests/replication/test_ack_modes.py -->
#### Acknowledgement modes test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

The contract contrasts `acks=0`, `acks=1`, and `acks=all`, then shrinks ISR both before and after append to expose two distinct failure windows.

##### Key test statement

```python
assert acknowledged.offset == 0
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

`acks=0` does not return offsets, `acks=1` waits for leader append, and `acks=all` requires the acknowledgement set plus `min.insync.replicas`.

### Why this mechanism is necessary

One word such as successful is ambiguous unless the producer knows which durability boundary acknowledged the write. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

The replica set checks admission before append, tracks required acknowledgers, advances replicas, and fails the waiter if ISR loses a required member before completion.

### Mechanism blocks

<!-- journey-file: src/minikafka/errors.py -->
<!-- journey-file: src/minikafka/producer/producer.py -->
<!-- journey-file: src/minikafka/replication/replica_set.py -->
#### Acknowledgement modes mechanism

##### What it is and why it appears

`acks=0` does not return offsets, `acks=1` waits for leader append, and `acks=all` requires the acknowledgement set plus `min.insync.replicas`.

##### Runtime role

The replica set checks admission before append, tracks required acknowledgers, advances replicas, and fails the waiter if ISR loses a required member before completion.

##### Statement understanding

Pre-append rejection prevents an under-replicated write; post-append failure prevents a leader-local tail from being mislabeled as fully acknowledged.



### Verification evidence

Run `uv run pytest -q $(cat journey/stages/13-ack-modes/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Pre-append rejection prevents an under-replicated write; post-append failure prevents a leader-local tail from being mislabeled as fully acknowledged.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 6](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/06-isr-and-fencing.md)

## 中文

### 目标

实现确认模式，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/errors.py`
- `src/minikafka/producer/producer.py`
- `src/minikafka/replication/replica_set.py`
- `tests/reliability/test_lost_acked_write.py`
- `tests/replication/test_ack_modes.py`

### 当前遇到的问题

如果 Producer 不知道是哪条持久性边界确认了写入，一个“成功”没有明确含义。

### 测试契约

#### 先看会坏在哪里

契约对比 `acks=0`、`acks=1` 与 `acks=all`，并分别在 Append 前后收缩 ISR，暴露两个不同失败窗口。

<!-- journey-file: tests/reliability/test_lost_acked_write.py -->
<!-- journey-file: tests/replication/test_ack_modes.py -->
#### 确认模式测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

契约对比 `acks=0`、`acks=1` 与 `acks=all`，并分别在 Append 前后收缩 ISR，暴露两个不同失败窗口。

##### 关键测试语句

```python
assert acknowledged.offset == 0
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

`acks=0` 不返回 Offset；`acks=1` 等待 Leader Append；`acks=all` 要求 Ack Set 并满足 `min.insync.replicas`。

### 为什么需要这个机制

如果 Producer 不知道是哪条持久性边界确认了写入，一个“成功”没有明确含义。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Replica Set 在 Append 前检查准入、记录 Required Acknowledger、推进 Replica，并在完成前 ISR 丢失必需成员时让 Waiter 失败。

### 机制板块

<!-- journey-file: src/minikafka/errors.py -->
<!-- journey-file: src/minikafka/producer/producer.py -->
<!-- journey-file: src/minikafka/replication/replica_set.py -->
#### 确认模式机制

##### 是什么，为什么现在需要

`acks=0` 不返回 Offset；`acks=1` 等待 Leader Append；`acks=all` 要求 Ack Set 并满足 `min.insync.replicas`。

##### 在运行时做什么

Replica Set 在 Append 前检查准入、记录 Required Acknowledger、推进 Replica，并在完成前 ISR 丢失必需成员时让 Waiter 失败。

##### 关键语句理解

Append 前拒绝避免写入进入低副本状态；Append 后失败避免把仅在 Leader Tail 的数据误标为完整确认。



### 验证证据

运行 `uv run pytest -q $(cat journey/stages/13-ack-modes/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

Append 前拒绝避免写入进入低副本状态；Append 后失败避免把仅在 Leader Tail 的数据误标为完整确认。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 6 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/06-isr-and-fencing.md)
