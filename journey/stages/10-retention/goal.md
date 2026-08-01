# Stage 10 · Prefix-only retention / 仅删除前缀的 Retention

<!-- journey: chapter=3 tests_added=1 -->

## English

### Goal

Build prefix-only retention and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/log/partition_log.py`
- `src/minikafka/log/retention.py`
- `src/minikafka/log/segment.py`
- `tests/log/test_retention.py`

### The problem at this point

Disk limits require deletion, but removing arbitrary bytes or the active segment would break offset continuity and recovery.

### Test contract

#### See the failure first

Time and size tests create several segments and prove deletion removes only eligible closed prefixes while preserving the active tail.

<!-- journey-file: tests/log/test_retention.py -->
#### Prefix-only retention test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Time and size tests create several segments and prove deletion removes only eligible closed prefixes while preserving the active tail.

##### Key test statement

```python
assert len(log.closed_segments) >= 3
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

Retention is physical deletion of old closed segments. Log start offset advances, while existing record offsets never change. The active segment is not a deletion candidate.

### Why this mechanism is necessary

Disk limits require deletion, but removing arbitrary bytes or the active segment would break offset continuity and recovery. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

The manager evaluates age or total bytes from oldest to newest, stops at the first ineligible boundary, deletes selected files, and refreshes the partition view.

### Mechanism blocks

<!-- journey-file: src/minikafka/log/partition_log.py -->
<!-- journey-file: src/minikafka/log/retention.py -->
<!-- journey-file: src/minikafka/log/segment.py -->
#### Prefix-only retention mechanism

##### What it is and why it appears

Retention is physical deletion of old closed segments. Log start offset advances, while existing record offsets never change. The active segment is not a deletion candidate.

##### Runtime role

The manager evaluates age or total bytes from oldest to newest, stops at the first ineligible boundary, deletes selected files, and refreshes the partition view.

##### Statement understanding

Deleting a prefix preserves a single monotonic log-start boundary; deleting a middle segment would create an unexplained physical hole.



### Verification evidence

Run `uv run pytest -q $(cat journey/stages/10-retention/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Deleting a prefix preserves a single monotonic log-start boundary; deleting a middle segment would create an unexplained physical hole.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 3](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/03-retention-compaction.md)

## 中文

### 目标

实现仅删除前缀的 Retention，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/log/partition_log.py`
- `src/minikafka/log/retention.py`
- `src/minikafka/log/segment.py`
- `tests/log/test_retention.py`

### 当前遇到的问题

磁盘限制要求删除数据，但任意删除字节或 Active Segment 会破坏 Offset 连续性与恢复。

### 测试契约

#### 先看会坏在哪里

时间与大小测试创建多个 Segment，并证明删除只作用于符合条件的 Closed Prefix，Active Tail 始终保留。

<!-- journey-file: tests/log/test_retention.py -->
#### 仅删除前缀的 Retention测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

时间与大小测试创建多个 Segment，并证明删除只作用于符合条件的 Closed Prefix，Active Tail 始终保留。

##### 关键测试语句

```python
assert len(log.closed_segments) >= 3
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Retention 是对旧 Closed Segment 的物理删除。Log Start Offset 前进，但已有 Record Offset 永不重编号。Active Segment 不是删除候选。

### 为什么需要这个机制

磁盘限制要求删除数据，但任意删除字节或 Active Segment 会破坏 Offset 连续性与恢复。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Manager 从旧到新评估 Age 或总字节，在第一个不符合边界处停止，删除选中的文件并刷新 Partition 视图。

### 机制板块

<!-- journey-file: src/minikafka/log/partition_log.py -->
<!-- journey-file: src/minikafka/log/retention.py -->
<!-- journey-file: src/minikafka/log/segment.py -->
#### 仅删除前缀的 Retention机制

##### 是什么，为什么现在需要

Retention 是对旧 Closed Segment 的物理删除。Log Start Offset 前进，但已有 Record Offset 永不重编号。Active Segment 不是删除候选。

##### 在运行时做什么

Manager 从旧到新评估 Age 或总字节，在第一个不符合边界处停止，删除选中的文件并刷新 Partition 视图。

##### 关键语句理解

删除前缀能保持唯一单调的 Log Start Boundary；删除中间 Segment 会制造无法解释的物理空洞。



### 验证证据

运行 `uv run pytest -q $(cat journey/stages/10-retention/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

删除前缀能保持唯一单调的 Log Start Boundary；删除中间 Segment 会制造无法解释的物理空洞。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 3 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/03-retention-compaction.md)
