# Stage 03 · Sparse offset lookup / 稀疏 Offset 查找

<!-- journey: chapter=2 tests_added=1 -->

## English

### Goal

Build sparse offset lookup and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/errors.py`
- `src/minikafka/log/__init__.py`
- `src/minikafka/log/index.py`
- `tests/unit/test_offset_index.py`

### The problem at this point

Scanning every batch from the start of a segment turns random offset reads into linear work.

### Test contract

#### See the failure first

Floor-lookup tests ask for offsets between entries and malformed-entry tests expose indexes that silently return a position after the requested record.

<!-- journey-file: tests/unit/test_offset_index.py -->
#### Sparse offset lookup test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Floor-lookup tests ask for offsets between entries and malformed-entry tests expose indexes that silently return a position after the requested record.

##### Key test statement

```python
assert index.floor_position(99) == 0
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

A sparse index maps selected relative offsets to byte positions. Floor lookup returns the closest indexed position not greater than the target, after which the log scans forward.

### Why this mechanism is necessary

Scanning every batch from the start of a segment turns random offset reads into linear work. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Append enforces monotonic entries, lookup performs ordered floor search, and truncation keeps index state aligned with a shortened log.

### Mechanism blocks

<!-- journey-file: src/minikafka/errors.py -->
<!-- journey-file: src/minikafka/log/index.py -->
#### Sparse offset lookup mechanism

##### What it is and why it appears

A sparse index maps selected relative offsets to byte positions. Floor lookup returns the closest indexed position not greater than the target, after which the log scans forward.

##### Runtime role

Append enforces monotonic entries, lookup performs ordered floor search, and truncation keeps index state aligned with a shortened log.

##### Statement understanding

Returning a floor rather than an exact match makes sparsity correct: the index narrows the scan without claiming to locate every record.

<!-- journey-file: src/minikafka/log/__init__.py -->
#### Package and project support

These files only keep the package boundary, dependencies, and test environment reproducible; they are supporting wiring rather than Kafka mechanism logic.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/03-offset-index/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Returning a floor rather than an exact match makes sparsity correct: the index narrows the scan without claiming to locate every record.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/02-the-log.md)

## 中文

### 目标

实现稀疏 Offset 查找，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/errors.py`
- `src/minikafka/log/__init__.py`
- `src/minikafka/log/index.py`
- `tests/unit/test_offset_index.py`

### 当前遇到的问题

每次按 Offset 读取都从 Segment 开头扫描，会把随机读取退化为线性工作。

### 测试契约

#### 先看会坏在哪里

Floor Lookup 测试查询两个条目之间的 Offset；损坏条目测试暴露会悄悄返回目标之后位置的索引。

<!-- journey-file: tests/unit/test_offset_index.py -->
#### 稀疏 Offset 查找测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

Floor Lookup 测试查询两个条目之间的 Offset；损坏条目测试暴露会悄悄返回目标之后位置的索引。

##### 关键测试语句

```python
assert index.floor_position(99) == 0
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

稀疏索引把部分相对 Offset 映射到字节位置。Floor Lookup 返回不大于目标的最近位置，日志再从那里向前扫描。

### 为什么需要这个机制

每次按 Offset 读取都从 Segment 开头扫描，会把随机读取退化为线性工作。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Append 保证条目单调，Lookup 执行有序 Floor Search，Truncate 让索引与缩短后的日志保持一致。

### 机制板块

<!-- journey-file: src/minikafka/errors.py -->
<!-- journey-file: src/minikafka/log/index.py -->
#### 稀疏 Offset 查找机制

##### 是什么，为什么现在需要

稀疏索引把部分相对 Offset 映射到字节位置。Floor Lookup 返回不大于目标的最近位置，日志再从那里向前扫描。

##### 在运行时做什么

Append 保证条目单调，Lookup 执行有序 Floor Search，Truncate 让索引与缩短后的日志保持一致。

##### 关键语句理解

返回 Floor 而非强求精确命中，让稀疏性仍然正确：索引只缩小扫描范围，不声称定位每条 Record。

<!-- journey-file: src/minikafka/log/__init__.py -->
#### 包与工程支撑

这些文件只负责让本 Stage 的包边界、依赖与测试环境可复现，不把脚手架误讲成 Kafka 机制。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/03-offset-index/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

返回 Floor 而非强求精确命中，让稀疏性仍然正确：索引只缩小扫描范围，不声称定位每条 Record。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/02-the-log.md)
