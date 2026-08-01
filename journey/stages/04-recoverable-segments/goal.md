# Stage 04 · Recoverable log segments / 可恢复日志段

<!-- journey: chapter=2 tests_added=2 -->

## English

### Goal

Build recoverable log segments and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/core/record.py`
- `src/minikafka/errors.py`
- `src/minikafka/log/recovery.py`
- `src/minikafka/log/segment.py`
- `tests/log/test_recovery.py`
- `tests/log/test_segment.py`

### The problem at this point

A process can stop between writing a frame and updating its index, leaving disk state that is neither clean nor safely ignorable.

### Test contract

#### See the failure first

Recovery tests append truncated bytes and corrupt frames, then distinguish a removable incomplete tail from corruption inside the durable prefix.

<!-- journey-file: tests/log/test_recovery.py -->
<!-- journey-file: tests/log/test_segment.py -->
#### Recoverable log segments test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Recovery tests append truncated bytes and corrupt frames, then distinguish a removable incomplete tail from corruption inside the durable prefix.

##### Key test statement

```python
assert recovered.size_bytes == valid_size
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

A Segment owns a log file and sparse index. Recovery scans complete batches, rebuilds derived index state, and truncates only an incomplete final frame.

### Why this mechanism is necessary

A process can stop between writing a frame and updating its index, leaving disk state that is neither clean nor safely ignorable. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Append writes encoded batches and periodically indexes offsets; reopen validates the durable prefix and reconstructs next-offset and byte-position state.

### Mechanism blocks

<!-- journey-file: src/minikafka/core/record.py -->
<!-- journey-file: src/minikafka/errors.py -->
<!-- journey-file: src/minikafka/log/recovery.py -->
<!-- journey-file: src/minikafka/log/segment.py -->
#### Recoverable log segments mechanism

##### What it is and why it appears

A Segment owns a log file and sparse index. Recovery scans complete batches, rebuilds derived index state, and truncates only an incomplete final frame.

##### Runtime role

Append writes encoded batches and periodically indexes offsets; reopen validates the durable prefix and reconstructs next-offset and byte-position state.

##### Statement understanding

Tail truncation is safe only when the decoder proves the failure begins at the final incomplete frame; mid-log corruption must remain visible.



### Verification evidence

Run `uv run pytest -q $(cat journey/stages/04-recoverable-segments/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Tail truncation is safe only when the decoder proves the failure begins at the final incomplete frame; mid-log corruption must remain visible.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/02-the-log.md)

## 中文

### 目标

实现可恢复日志段，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/core/record.py`
- `src/minikafka/errors.py`
- `src/minikafka/log/recovery.py`
- `src/minikafka/log/segment.py`
- `tests/log/test_recovery.py`
- `tests/log/test_segment.py`

### 当前遇到的问题

进程可能在写 Frame 与更新索引之间停止，留下既不完整又不能安全忽略的磁盘状态。

### 测试契约

#### 先看会坏在哪里

恢复测试追加截断字节与损坏 Frame，并区分可移除的不完整尾部和持久前缀内部的损坏。

<!-- journey-file: tests/log/test_recovery.py -->
<!-- journey-file: tests/log/test_segment.py -->
#### 可恢复日志段测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

恢复测试追加截断字节与损坏 Frame，并区分可移除的不完整尾部和持久前缀内部的损坏。

##### 关键测试语句

```python
assert recovered.size_bytes == valid_size
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Segment 拥有日志文件与稀疏索引。恢复过程扫描完整 Batch、重建派生索引，并且只截断最后一个不完整 Frame。

### 为什么需要这个机制

进程可能在写 Frame 与更新索引之间停止，留下既不完整又不能安全忽略的磁盘状态。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Append 写入编码 Batch 并周期性建立索引；重开时验证持久前缀并重建 Next Offset 与 Byte Position。

### 机制板块

<!-- journey-file: src/minikafka/core/record.py -->
<!-- journey-file: src/minikafka/errors.py -->
<!-- journey-file: src/minikafka/log/recovery.py -->
<!-- journey-file: src/minikafka/log/segment.py -->
#### 可恢复日志段机制

##### 是什么，为什么现在需要

Segment 拥有日志文件与稀疏索引。恢复过程扫描完整 Batch、重建派生索引，并且只截断最后一个不完整 Frame。

##### 在运行时做什么

Append 写入编码 Batch 并周期性建立索引；重开时验证持久前缀并重建 Next Offset 与 Byte Position。

##### 关键语句理解

只有 Decoder 证明失败始于最后一个不完整 Frame 时，尾部截断才安全；日志中段损坏必须暴露出来。



### 验证证据

运行 `uv run pytest -q $(cat journey/stages/04-recoverable-segments/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

只有 Decoder 证明失败始于最后一个不完整 Frame 时，尾部截断才安全；日志中段损坏必须暴露出来。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/02-the-log.md)
