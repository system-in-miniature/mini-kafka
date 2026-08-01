# Stage 14 · Promotion and epoch fencing / 晋升与 Epoch Fencing

<!-- journey: chapter=6 tests_added=3 -->

## English

### Goal

Build promotion and epoch fencing and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/core/cluster.py`
- `src/minikafka/errors.py`
- `src/minikafka/replication/replica_set.py`
- `tests/reliability/test_lost_acked_write.py`
- `tests/replication/test_divergent_tail.py`
- `tests/replication/test_promotion.py`

### The problem at this point

After failover, an old leader may still accept traffic and carry a suffix the new leader never committed.

### Test contract

#### See the failure first

Promotion tests reject non-ISR candidates, increment epochs, issue stale requests, and force the old leader to truncate an uncommitted tail.

<!-- journey-file: tests/reliability/test_lost_acked_write.py -->
<!-- journey-file: tests/replication/test_divergent_tail.py -->
<!-- journey-file: tests/replication/test_promotion.py -->
#### Promotion and epoch fencing test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Promotion tests reject non-ISR candidates, increment epochs, issue stale requests, and force the old leader to truncate an uncommitted tail.

##### Key test statement

```python
assert cluster.leader_log(tp).leo == 0
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

Leader epoch versions authority. Clean promotion chooses an eligible ISR replica. Divergent data after HW is uncommitted and must not survive reconciliation.

### Why this mechanism is necessary

After failover, an old leader may still accept traffic and carry a suffix the new leader never committed. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Promotion validates eligibility, increments and persists the epoch, changes metadata, truncates replicas to the committed boundary, and fences requests carrying older epochs.

### Mechanism blocks

<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/errors.py -->
<!-- journey-file: src/minikafka/replication/replica_set.py -->
#### Promotion and epoch fencing mechanism

##### What it is and why it appears

Leader epoch versions authority. Clean promotion chooses an eligible ISR replica. Divergent data after HW is uncommitted and must not survive reconciliation.

##### Runtime role

Promotion validates eligibility, increments and persists the epoch, changes metadata, truncates replicas to the committed boundary, and fences requests carrying older epochs.

##### Statement understanding

Comparing the request epoch before mutation turns stale authority into a typed failure; truncating to HW preserves only the agreed prefix.



### Verification evidence

Run `uv run pytest -q $(cat journey/stages/14-promotion-fencing/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Comparing the request epoch before mutation turns stale authority into a typed failure; truncating to HW preserves only the agreed prefix.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 6](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/06-isr-and-fencing.md)

## 中文

### 目标

实现晋升与 Epoch Fencing，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/core/cluster.py`
- `src/minikafka/errors.py`
- `src/minikafka/replication/replica_set.py`
- `tests/reliability/test_lost_acked_write.py`
- `tests/replication/test_divergent_tail.py`
- `tests/replication/test_promotion.py`

### 当前遇到的问题

Failover 后旧 Leader 可能仍接收流量，并携带新 Leader 从未提交的后缀。

### 测试契约

#### 先看会坏在哪里

Promotion 测试拒绝非 ISR Candidate、增加 Epoch、发送过期请求，并迫使旧 Leader 截断未提交 Tail。

<!-- journey-file: tests/reliability/test_lost_acked_write.py -->
<!-- journey-file: tests/replication/test_divergent_tail.py -->
<!-- journey-file: tests/replication/test_promotion.py -->
#### 晋升与 Epoch Fencing测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

Promotion 测试拒绝非 ISR Candidate、增加 Epoch、发送过期请求，并迫使旧 Leader 截断未提交 Tail。

##### 关键测试语句

```python
assert cluster.leader_log(tp).leo == 0
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Leader Epoch 为权威建版本；Clean Promotion 选择合格 ISR Replica；HW 之后的 Divergent Data 未提交，协调时不得保留。

### 为什么需要这个机制

Failover 后旧 Leader 可能仍接收流量，并携带新 Leader 从未提交的后缀。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Promotion 校验资格、增加并持久化 Epoch、修改 Metadata、把 Replica 截到 Commit Boundary，并 Fencing 携带旧 Epoch 的请求。

### 机制板块

<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/errors.py -->
<!-- journey-file: src/minikafka/replication/replica_set.py -->
#### 晋升与 Epoch Fencing机制

##### 是什么，为什么现在需要

Leader Epoch 为权威建版本；Clean Promotion 选择合格 ISR Replica；HW 之后的 Divergent Data 未提交，协调时不得保留。

##### 在运行时做什么

Promotion 校验资格、增加并持久化 Epoch、修改 Metadata、把 Replica 截到 Commit Boundary，并 Fencing 携带旧 Epoch 的请求。

##### 关键语句理解

在修改前比较 Request Epoch，把过期权威变成类型化失败；截断到 HW 只保留已达成共识的前缀。



### 验证证据

运行 `uv run pytest -q $(cat journey/stages/14-promotion-fencing/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

在修改前比较 Request Epoch，把过期权威变成类型化失败；截断到 HW 只保留已达成共识的前缀。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 6 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/06-isr-and-fencing.md)
