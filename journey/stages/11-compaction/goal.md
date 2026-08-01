# Stage 11 · Keyed log compaction / 按 Key 的日志压实

<!-- journey: chapter=3 tests_added=3 -->

## English

### Goal

Build keyed log compaction and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/core/batch.py`
- `src/minikafka/core/batch_codec.py`
- `src/minikafka/log/compaction.py`
- `src/minikafka/log/partition_log.py`
- `src/minikafka/log/segment.py`
- `tests/log/test_compaction.py`
- `tests/reliability/test_compaction_swap.py`
- `tests/unit/test_batch_codec.py`

### The problem at this point

Retention controls age and size but cannot preserve the latest state per key while discarding superseded values.

### Test contract

#### See the failure first

Compaction tests include duplicate keys, tombstones, offset gaps, and an injected swap failure so a partially replaced segment set cannot become authoritative.

<!-- journey-file: tests/log/test_compaction.py -->
<!-- journey-file: tests/reliability/test_compaction_swap.py -->
<!-- journey-file: tests/unit/test_batch_codec.py -->
#### Keyed log compaction test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Compaction tests include duplicate keys, tombstones, offset gaps, and an injected swap failure so a partially replaced segment set cannot become authoritative.

##### Key test statement

```python
assert [
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

Compaction retains the latest record for each key in a chosen closed range. Tombstones represent deletion. Logical offsets remain unchanged even when physical records disappear.

### Why this mechanism is necessary

Retention controls age and size but cannot preserve the latest state per key while discarding superseded values. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

The compactor scans newest knowledge, rewrites retained records to temporary segments, fsyncs them, atomically swaps the range, and only then removes old files.

### Mechanism blocks

<!-- journey-file: src/minikafka/core/batch.py -->
<!-- journey-file: src/minikafka/core/batch_codec.py -->
<!-- journey-file: src/minikafka/log/compaction.py -->
<!-- journey-file: src/minikafka/log/partition_log.py -->
<!-- journey-file: src/minikafka/log/segment.py -->
#### Keyed log compaction mechanism

##### What it is and why it appears

Compaction retains the latest record for each key in a chosen closed range. Tombstones represent deletion. Logical offsets remain unchanged even when physical records disappear.

##### Runtime role

The compactor scans newest knowledge, rewrites retained records to temporary segments, fsyncs them, atomically swaps the range, and only then removes old files.

##### Statement understanding

Preserving original offsets makes compaction a storage rewrite rather than a new log; the atomic swap makes either old or new segments recoverable.



### Verification evidence

Run `uv run pytest -q $(cat journey/stages/11-compaction/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Preserving original offsets makes compaction a storage rewrite rather than a new log; the atomic swap makes either old or new segments recoverable.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 3](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/03-retention-compaction.md)

## 中文

### 目标

实现按 Key 的日志压实，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/core/batch.py`
- `src/minikafka/core/batch_codec.py`
- `src/minikafka/log/compaction.py`
- `src/minikafka/log/partition_log.py`
- `src/minikafka/log/segment.py`
- `tests/log/test_compaction.py`
- `tests/reliability/test_compaction_swap.py`
- `tests/unit/test_batch_codec.py`

### 当前遇到的问题

Retention 控制年龄与大小，却无法在丢弃旧值时保留每个 Key 的最新状态。

### 测试契约

#### 先看会坏在哪里

Compaction 测试包含重复 Key、Tombstone、Offset Gap 与注入的 Swap Failure，避免部分替换的 Segment 集合成为权威状态。

<!-- journey-file: tests/log/test_compaction.py -->
<!-- journey-file: tests/reliability/test_compaction_swap.py -->
<!-- journey-file: tests/unit/test_batch_codec.py -->
#### 按 Key 的日志压实测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

Compaction 测试包含重复 Key、Tombstone、Offset Gap 与注入的 Swap Failure，避免部分替换的 Segment 集合成为权威状态。

##### 关键测试语句

```python
assert [
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Compaction 在选定 Closed Range 中保留每个 Key 的最新 Record；Tombstone 表示删除；即使物理记录消失，逻辑 Offset 也不变。

### 为什么需要这个机制

Retention 控制年龄与大小，却无法在丢弃旧值时保留每个 Key 的最新状态。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Compactor 扫描最新 Key 状态，把保留记录写入临时 Segment、Fsync、原子交换范围，之后才删除旧文件。

### 机制板块

<!-- journey-file: src/minikafka/core/batch.py -->
<!-- journey-file: src/minikafka/core/batch_codec.py -->
<!-- journey-file: src/minikafka/log/compaction.py -->
<!-- journey-file: src/minikafka/log/partition_log.py -->
<!-- journey-file: src/minikafka/log/segment.py -->
#### 按 Key 的日志压实机制

##### 是什么，为什么现在需要

Compaction 在选定 Closed Range 中保留每个 Key 的最新 Record；Tombstone 表示删除；即使物理记录消失，逻辑 Offset 也不变。

##### 在运行时做什么

Compactor 扫描最新 Key 状态，把保留记录写入临时 Segment、Fsync、原子交换范围，之后才删除旧文件。

##### 关键语句理解

保留原 Offset 让 Compaction 只是存储重写而非新日志；原子 Swap 确保恢复时看到完整旧版本或新版本。



### 验证证据

运行 `uv run pytest -q $(cat journey/stages/11-compaction/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

保留原 Offset 让 Compaction 只是存储重写而非新日志；原子 Swap 确保恢复时看到完整旧版本或新版本。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 3 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/03-retention-compaction.md)
