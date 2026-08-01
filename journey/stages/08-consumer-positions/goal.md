# Stage 08 · Consumer position and replay / Consumer Position 与重放

<!-- journey: chapter=7 tests_added=3 -->

## English

### Goal

Build consumer position and replay and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/consumer/__init__.py`
- `src/minikafka/consumer/consumer.py`
- `src/minikafka/consumer/offsets.py`
- `src/minikafka/core/cluster.py`
- `tests/consumer/test_delivery_semantics.py`
- `tests/consumer/test_positions.py`
- `tests/reliability/test_offset_restart.py`

### The problem at this point

Reading a record and declaring it processed are different events; collapsing them makes crash behavior impossible to control.

### Test contract

#### See the failure first

Delivery tests crash once before commit and once after commit, making replay and skip behavior visible instead of describing it abstractly.

<!-- journey-file: tests/consumer/test_delivery_semantics.py -->
<!-- journey-file: tests/consumer/test_positions.py -->
<!-- journey-file: tests/reliability/test_offset_restart.py -->
#### Consumer position and replay test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Delivery tests crash once before commit and once after commit, making replay and skip behavior visible instead of describing it abstractly.

##### Key test statement

```python
assert (await first.poll(1))[0].value == b"work"
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

Position is the next offset in one consumer instance. A committed offset is durable group progress. Earliest/latest reset policy supplies a starting point only when no commit exists.

### Why this mechanism is necessary

Reading a record and declaring it processed are different events; collapsing them makes crash behavior impossible to control. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Poll advances local positions as records are returned; commit persists selected next offsets; a reopened consumer initializes from committed state or reset policy.

### Mechanism blocks

<!-- journey-file: src/minikafka/consumer/consumer.py -->
<!-- journey-file: src/minikafka/consumer/offsets.py -->
<!-- journey-file: src/minikafka/core/cluster.py -->
#### Consumer position and replay mechanism

##### What it is and why it appears

Position is the next offset in one consumer instance. A committed offset is durable group progress. Earliest/latest reset policy supplies a starting point only when no commit exists.

##### Runtime role

Poll advances local positions as records are returned; commit persists selected next offsets; a reopened consumer initializes from committed state or reset policy.

##### Statement understanding

Advancing position during poll is safe because durable progress changes only at commit; this separation defines at-least-once and at-most-once failure windows.

<!-- journey-file: src/minikafka/consumer/__init__.py -->
#### Package and project support

These files only keep the package boundary, dependencies, and test environment reproducible; they are supporting wiring rather than Kafka mechanism logic.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/08-consumer-positions/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Advancing position during poll is safe because durable progress changes only at commit; this separation defines at-least-once and at-most-once failure windows.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 7](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/07-consumer-groups.md)

## 中文

### 目标

实现Consumer Position 与重放，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/consumer/__init__.py`
- `src/minikafka/consumer/consumer.py`
- `src/minikafka/consumer/offsets.py`
- `src/minikafka/core/cluster.py`
- `tests/consumer/test_delivery_semantics.py`
- `tests/consumer/test_positions.py`
- `tests/reliability/test_offset_restart.py`

### 当前遇到的问题

读取 Record 与声明处理完成是两个事件；把它们合并会让崩溃行为无法控制。

### 测试契约

#### 先看会坏在哪里

投递测试分别在 Commit 前后模拟崩溃，让 Replay 与 Skip 行为直接可见。

<!-- journey-file: tests/consumer/test_delivery_semantics.py -->
<!-- journey-file: tests/consumer/test_positions.py -->
<!-- journey-file: tests/reliability/test_offset_restart.py -->
#### Consumer Position 与重放测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

投递测试分别在 Commit 前后模拟崩溃，让 Replay 与 Skip 行为直接可见。

##### 关键测试语句

```python
assert (await first.poll(1))[0].value == b"work"
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Position 是单个 Consumer 实例的下一 Offset；Committed Offset 是持久 Group 进度；只有不存在 Commit 时，Earliest/Latest 才提供起点。

### 为什么需要这个机制

读取 Record 与声明处理完成是两个事件；把它们合并会让崩溃行为无法控制。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Poll 返回记录时推进本地 Position；Commit 持久化选定 Next Offset；重开 Consumer 从 Commit 或 Reset Policy 初始化。

### 机制板块

<!-- journey-file: src/minikafka/consumer/consumer.py -->
<!-- journey-file: src/minikafka/consumer/offsets.py -->
<!-- journey-file: src/minikafka/core/cluster.py -->
#### Consumer Position 与重放机制

##### 是什么，为什么现在需要

Position 是单个 Consumer 实例的下一 Offset；Committed Offset 是持久 Group 进度；只有不存在 Commit 时，Earliest/Latest 才提供起点。

##### 在运行时做什么

Poll 返回记录时推进本地 Position；Commit 持久化选定 Next Offset；重开 Consumer 从 Commit 或 Reset Policy 初始化。

##### 关键语句理解

Poll 时推进 Position 是安全的，因为持久进度只在 Commit 时变化；这一区分定义了 At-least-once 与 At-most-once 的失败窗口。

<!-- journey-file: src/minikafka/consumer/__init__.py -->
#### 包与工程支撑

这些文件只负责让本 Stage 的包边界、依赖与测试环境可复现，不把脚手架误讲成 Kafka 机制。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/08-consumer-positions/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

Poll 时推进 Position 是安全的，因为持久进度只在 Commit 时变化；这一区分定义了 At-least-once 与 At-most-once 的失败窗口。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 7 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/07-consumer-groups.md)
