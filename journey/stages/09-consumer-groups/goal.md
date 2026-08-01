# Stage 09 · Consumer-group ownership / Consumer Group 所有权

<!-- journey: chapter=7 tests_added=3 -->

## English

### Goal

Build consumer-group ownership and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/config.py`
- `src/minikafka/consumer/assignor.py`
- `src/minikafka/consumer/consumer.py`
- `src/minikafka/consumer/group.py`
- `src/minikafka/core/cluster.py`
- `src/minikafka/errors.py`
- `tests/consumer/test_generation_fencing.py`
- `tests/consumer/test_group_rebalance.py`
- `tests/unit/test_assignor.py`

### The problem at this point

Independent consumers can read the same partition concurrently unless the group has one authoritative assignment and generation.

### Test contract

#### See the failure first

Rebalance tests join, leave, expire heartbeats, and then attempt stale-generation and non-owner commits.

<!-- journey-file: tests/consumer/test_generation_fencing.py -->
<!-- journey-file: tests/consumer/test_group_rebalance.py -->
<!-- journey-file: tests/unit/test_assignor.py -->
#### Consumer-group ownership test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Rebalance tests join, leave, expire heartbeats, and then attempt stale-generation and non-owner commits.

##### Key test statement

```python
assert len(first.assignment) == 4
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

A group generation versions an assignment. Heartbeats extend membership leases. The assignor gives each subscribed partition one owner. Fencing rejects actions based on old ownership.

### Why this mechanism is necessary

Independent consumers can read the same partition concurrently unless the group has one authoritative assignment and generation. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Join or membership expiry recomputes assignments and increments generation; consumers refresh their view; commit checks member, generation, and partition ownership together.

### Mechanism blocks

<!-- journey-file: src/minikafka/config.py -->
<!-- journey-file: src/minikafka/consumer/assignor.py -->
<!-- journey-file: src/minikafka/consumer/consumer.py -->
<!-- journey-file: src/minikafka/consumer/group.py -->
<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/errors.py -->
#### Consumer-group ownership mechanism

##### What it is and why it appears

A group generation versions an assignment. Heartbeats extend membership leases. The assignor gives each subscribed partition one owner. Fencing rejects actions based on old ownership.

##### Runtime role

Join or membership expiry recomputes assignments and increments generation; consumers refresh their view; commit checks member, generation, and partition ownership together.

##### Statement understanding

The generation check must be atomic with ownership validation, or a consumer can commit after the assignment it observed has already disappeared.



### Verification evidence

Run `uv run pytest -q $(cat journey/stages/09-consumer-groups/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

The generation check must be atomic with ownership validation, or a consumer can commit after the assignment it observed has already disappeared.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 7](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/07-consumer-groups.md)

## 中文

### 目标

实现Consumer Group 所有权，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/config.py`
- `src/minikafka/consumer/assignor.py`
- `src/minikafka/consumer/consumer.py`
- `src/minikafka/consumer/group.py`
- `src/minikafka/core/cluster.py`
- `src/minikafka/errors.py`
- `tests/consumer/test_generation_fencing.py`
- `tests/consumer/test_group_rebalance.py`
- `tests/unit/test_assignor.py`

### 当前遇到的问题

如果 Group 没有权威 Assignment 与 Generation，多个 Consumer 会并发读取同一 Partition。

### 测试契约

#### 先看会坏在哪里

Rebalance 测试执行 Join、Leave、Heartbeat Expiry，并尝试旧 Generation 与非 Owner Commit。

<!-- journey-file: tests/consumer/test_generation_fencing.py -->
<!-- journey-file: tests/consumer/test_group_rebalance.py -->
<!-- journey-file: tests/unit/test_assignor.py -->
#### Consumer Group 所有权测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

Rebalance 测试执行 Join、Leave、Heartbeat Expiry，并尝试旧 Generation 与非 Owner Commit。

##### 关键测试语句

```python
assert len(first.assignment) == 4
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Group Generation 为 Assignment 建版本；Heartbeat 延长成员 Lease；Assignor 让每个订阅 Partition 只有一个 Owner；Fencing 拒绝旧所有权操作。

### 为什么需要这个机制

如果 Group 没有权威 Assignment 与 Generation，多个 Consumer 会并发读取同一 Partition。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Join 或成员过期会重算 Assignment 并增加 Generation；Consumer 刷新视图；Commit 同时检查 Member、Generation 与 Partition Ownership。

### 机制板块

<!-- journey-file: src/minikafka/config.py -->
<!-- journey-file: src/minikafka/consumer/assignor.py -->
<!-- journey-file: src/minikafka/consumer/consumer.py -->
<!-- journey-file: src/minikafka/consumer/group.py -->
<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/errors.py -->
#### Consumer Group 所有权机制

##### 是什么，为什么现在需要

Group Generation 为 Assignment 建版本；Heartbeat 延长成员 Lease；Assignor 让每个订阅 Partition 只有一个 Owner；Fencing 拒绝旧所有权操作。

##### 在运行时做什么

Join 或成员过期会重算 Assignment 并增加 Generation；Consumer 刷新视图；Commit 同时检查 Member、Generation 与 Partition Ownership。

##### 关键语句理解

Generation 检查必须与 Ownership 校验原子完成，否则 Consumer 会在其观察到的 Assignment 已失效后仍然 Commit。



### 验证证据

运行 `uv run pytest -q $(cat journey/stages/09-consumer-groups/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

Generation 检查必须与 Ownership 校验原子完成，否则 Consumer 会在其观察到的 Assignment 已失效后仍然 Commit。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 7 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/07-consumer-groups.md)
