# Stage 16 · Transactional records and offsets / 事务化 Record 与 Offset

<!-- journey: chapter=8 tests_added=4 -->

## English

### Goal

Build transactional records and offsets and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/consumer/offsets.py`
- `src/minikafka/core/cluster.py`
- `src/minikafka/transaction/__init__.py`
- `src/minikafka/transaction/journal.py`
- `src/minikafka/transaction/manager.py`
- `src/minikafka/transaction/model.py`
- `tests/reliability/test_transaction_restart.py`
- `tests/transaction/test_abort.py`
- `tests/transaction/test_offsets.py`
- `tests/transaction/test_visibility.py`

### The problem at this point

Publishing output and committing input progress separately can expose partial work after a crash.

### Test contract

#### See the failure first

Visibility tests pause between prepare and commit, abort tests hide data, offset tests publish output and input progress together, and journal tests cut the durable tail.

<!-- journey-file: tests/reliability/test_transaction_restart.py -->
<!-- journey-file: tests/transaction/test_abort.py -->
<!-- journey-file: tests/transaction/test_offsets.py -->
<!-- journey-file: tests/transaction/test_visibility.py -->
#### Transactional records and offsets test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Visibility tests pause between prepare and commit, abort tests hide data, offset tests publish output and input progress together, and journal tests cut the durable tail.

##### Key test statement

```python
assert [r.value for r in reopened.fetch(
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

A transaction has an epoch and state. Control batches mark commit or abort. `read_committed` filters unresolved and aborted data. The journal drives recovery of prepared decisions.

### Why this mechanism is necessary

Publishing output and committing input progress separately can expose partial work after a crash. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Begin fences old epochs, send appends transactional batches, prepare durably records intent, commit writes markers and offsets, and recovery completes or aborts incomplete work deterministically.

### Mechanism blocks

<!-- journey-file: src/minikafka/consumer/offsets.py -->
<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/transaction/journal.py -->
<!-- journey-file: src/minikafka/transaction/manager.py -->
<!-- journey-file: src/minikafka/transaction/model.py -->
#### Transactional records and offsets mechanism

##### What it is and why it appears

A transaction has an epoch and state. Control batches mark commit or abort. `read_committed` filters unresolved and aborted data. The journal drives recovery of prepared decisions.

##### Runtime role

Begin fences old epochs, send appends transactional batches, prepare durably records intent, commit writes markers and offsets, and recovery completes or aborts incomplete work deterministically.

##### Statement understanding

Output visibility and input-offset publication must share the commit decision; otherwise recovery can duplicate output or skip input.

<!-- journey-file: src/minikafka/transaction/__init__.py -->
#### Package and project support

These files only keep the package boundary, dependencies, and test environment reproducible; they are supporting wiring rather than Kafka mechanism logic.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/16-transactions/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Output visibility and input-offset publication must share the commit decision; otherwise recovery can duplicate output or skip input.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 8](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/08-transactions.md)

## 中文

### 目标

实现事务化 Record 与 Offset，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/consumer/offsets.py`
- `src/minikafka/core/cluster.py`
- `src/minikafka/transaction/__init__.py`
- `src/minikafka/transaction/journal.py`
- `src/minikafka/transaction/manager.py`
- `src/minikafka/transaction/model.py`
- `tests/reliability/test_transaction_restart.py`
- `tests/transaction/test_abort.py`
- `tests/transaction/test_offsets.py`
- `tests/transaction/test_visibility.py`

### 当前遇到的问题

分别发布输出与提交输入进度，会在崩溃后暴露部分完成的工作。

### 测试契约

#### 先看会坏在哪里

可见性测试停在 Prepare 与 Commit 之间；Abort 测试隐藏数据；Offset 测试让输出与输入进度一起发布；Journal 测试截断持久尾部。

<!-- journey-file: tests/reliability/test_transaction_restart.py -->
<!-- journey-file: tests/transaction/test_abort.py -->
<!-- journey-file: tests/transaction/test_offsets.py -->
<!-- journey-file: tests/transaction/test_visibility.py -->
#### 事务化 Record 与 Offset测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

可见性测试停在 Prepare 与 Commit 之间；Abort 测试隐藏数据；Offset 测试让输出与输入进度一起发布；Journal 测试截断持久尾部。

##### 关键测试语句

```python
assert [r.value for r in reopened.fetch(
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Transaction 具有 Epoch 与 State；Control Batch 标记 Commit 或 Abort；`read_committed` 过滤未决与已中止数据；Journal 驱动 Prepared Decision 恢复。

### 为什么需要这个机制

分别发布输出与提交输入进度，会在崩溃后暴露部分完成的工作。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Begin Fencing 旧 Epoch；Send 追加事务 Batch；Prepare 持久记录意图；Commit 写 Marker 与 Offset；Recovery 确定性完成或中止未完成工作。

### 机制板块

<!-- journey-file: src/minikafka/consumer/offsets.py -->
<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/transaction/journal.py -->
<!-- journey-file: src/minikafka/transaction/manager.py -->
<!-- journey-file: src/minikafka/transaction/model.py -->
#### 事务化 Record 与 Offset机制

##### 是什么，为什么现在需要

Transaction 具有 Epoch 与 State；Control Batch 标记 Commit 或 Abort；`read_committed` 过滤未决与已中止数据；Journal 驱动 Prepared Decision 恢复。

##### 在运行时做什么

Begin Fencing 旧 Epoch；Send 追加事务 Batch；Prepare 持久记录意图；Commit 写 Marker 与 Offset；Recovery 确定性完成或中止未完成工作。

##### 关键语句理解

输出可见性与输入 Offset 发布必须共享同一 Commit Decision，否则恢复会重复输出或跳过输入。

<!-- journey-file: src/minikafka/transaction/__init__.py -->
#### 包与工程支撑

这些文件只负责让本 Stage 的包边界、依赖与测试环境可复现，不把脚手架误讲成 Kafka 机制。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/16-transactions/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

输出可见性与输入 Offset 发布必须共享同一 Commit Decision，否则恢复会重复输出或跳过输入。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 8 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/08-transactions.md)
