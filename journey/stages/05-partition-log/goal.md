# Stage 05 · Segmented partition log / 分段 Partition Log

<!-- journey: chapter=2 tests_added=2 -->

## English

### Goal

Build segmented partition log and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/config.py`
- `src/minikafka/log/partition_log.py`
- `tests/log/test_partition_log.py`
- `tests/reliability/test_log_restart.py`

### The problem at this point

One ever-growing file makes retention, recovery, and bounded lookup difficult to reason about or operate.

### Test contract

#### See the failure first

Rolling and restart tests cross segment boundaries, then prove offsets and reads remain continuous after the process reopens the directory.

<!-- journey-file: tests/log/test_partition_log.py -->
<!-- journey-file: tests/reliability/test_log_restart.py -->
#### Segmented partition log test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Rolling and restart tests cross segment boundaries, then prove offsets and reads remain continuous after the process reopens the directory.

##### Key test statement

```python
assert len(log.segments) >= 2
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

A PartitionLog is an ordered sequence of immutable closed segments plus one active segment. Base offsets define the global address space.

### Why this mechanism is necessary

One ever-growing file makes retention, recovery, and bounded lookup difficult to reason about or operate. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Append rolls before a size limit would be exceeded; reads choose a candidate segment and scan batches; reopen orders files and recovers the active tail.

### Mechanism blocks

<!-- journey-file: src/minikafka/config.py -->
<!-- journey-file: src/minikafka/log/partition_log.py -->
#### Segmented partition log mechanism

##### What it is and why it appears

A PartitionLog is an ordered sequence of immutable closed segments plus one active segment. Base offsets define the global address space.

##### Runtime role

Append rolls before a size limit would be exceeded; reads choose a candidate segment and scan batches; reopen orders files and recovers the active tail.

##### Statement understanding

Rolling before append keeps every completed segment within its configured bound while preserving one monotonic partition offset sequence.



### Verification evidence

Run `uv run pytest -q $(cat journey/stages/05-partition-log/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Rolling before append keeps every completed segment within its configured bound while preserving one monotonic partition offset sequence.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/02-the-log.md)

## 中文

### 目标

实现分段 Partition Log，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/config.py`
- `src/minikafka/log/partition_log.py`
- `tests/log/test_partition_log.py`
- `tests/reliability/test_log_restart.py`

### 当前遇到的问题

单个无限增长文件会让 Retention、Recovery 与有界查找难以推理和运维。

### 测试契约

#### 先看会坏在哪里

滚动与重启测试跨越 Segment 边界，并证明进程重开目录后 Offset 与读取仍连续。

<!-- journey-file: tests/log/test_partition_log.py -->
<!-- journey-file: tests/reliability/test_log_restart.py -->
#### 分段 Partition Log测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

滚动与重启测试跨越 Segment 边界，并证明进程重开目录后 Offset 与读取仍连续。

##### 关键测试语句

```python
assert len(log.segments) >= 2
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

PartitionLog 是若干不可变 Closed Segment 加一个 Active Segment 的有序序列。Base Offset 定义全局地址空间。

### 为什么需要这个机制

单个无限增长文件会让 Retention、Recovery 与有界查找难以推理和运维。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Append 在超过大小限制前 Roll；Read 选择候选 Segment 再扫描 Batch；重开时排序文件并恢复 Active Tail。

### 机制板块

<!-- journey-file: src/minikafka/config.py -->
<!-- journey-file: src/minikafka/log/partition_log.py -->
#### 分段 Partition Log机制

##### 是什么，为什么现在需要

PartitionLog 是若干不可变 Closed Segment 加一个 Active Segment 的有序序列。Base Offset 定义全局地址空间。

##### 在运行时做什么

Append 在超过大小限制前 Roll；Read 选择候选 Segment 再扫描 Batch；重开时排序文件并恢复 Active Tail。

##### 关键语句理解

在 Append 前滚动，使每个完成 Segment 保持在配置边界内，同时维持单调的 Partition Offset 序列。



### 验证证据

运行 `uv run pytest -q $(cat journey/stages/05-partition-log/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

在 Append 前滚动，使每个完成 Segment 保持在配置边界内，同时维持单调的 Partition Offset 序列。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/02-the-log.md)
