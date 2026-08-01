# Stage 19 · ISR rejoin regression / ISR Rejoin 回归

<!-- journey: chapter=6 tests_added=1 -->

## English

### Goal

Build isr rejoin regression and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/replication/replica_set.py`
- `tests/replication/test_promotion.py`

### The problem at this point

A follower can be close to the leader LEO yet still sit behind the committed high watermark, so LEO-only rejoin admits an unsafe election candidate.

### Test contract

#### See the failure first

The regression constructs a replica behind HW, asks it to rejoin, and then attempts promotion; the old predicate would let both operations succeed.

<!-- journey-file: tests/replication/test_promotion.py -->
#### ISR rejoin regression test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

The regression constructs a replica behind HW, asks it to rejoin, and then attempts promotion; the old predicate would let both operations succeed.

##### Key test statement

```python
assert replica_set.high_watermark == 1
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

ISR membership is a safety claim, not a freshness hint. A rejoining replica must contain the entire committed prefix before it can acknowledge or become leader.

### Why this mechanism is necessary

A follower can be close to the leader LEO yet still sit behind the committed high watermark, so LEO-only rejoin admits an unsafe election candidate. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Membership refresh compares follower LEO with both lag policy and HW; only replicas at or beyond HW reenter the set used by acknowledgement and election.

### Mechanism blocks

<!-- journey-file: src/minikafka/replication/replica_set.py -->
#### ISR rejoin regression mechanism

##### What it is and why it appears

ISR membership is a safety claim, not a freshness hint. A rejoining replica must contain the entire committed prefix before it can acknowledge or become leader.

##### Runtime role

Membership refresh compares follower LEO with both lag policy and HW; only replicas at or beyond HW reenter the set used by acknowledgement and election.

##### Statement understanding

The `leo >= high_watermark` conjunct is the missing safety gate: without it, shrinking leader progress can make an incomplete replica appear caught up.



### Verification evidence

Run `uv run pytest -q $(cat journey/stages/19-isr-regression/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

The `leo >= high_watermark` conjunct is the missing safety gate: without it, shrinking leader progress can make an incomplete replica appear caught up.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 6](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/06-isr-and-fencing.md)

## 中文

### 目标

实现ISR Rejoin 回归，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/replication/replica_set.py`
- `tests/replication/test_promotion.py`

### 当前遇到的问题

Follower 可能接近 Leader LEO 却仍落后于已提交 High Watermark，因此只看 LEO 的 Rejoin 会接纳不安全的选举候选。

### 测试契约

#### 先看会坏在哪里

回归测试构造落后 HW 的 Replica，尝试让它 Rejoin，再尝试 Promotion；旧谓词会让两步都成功。

<!-- journey-file: tests/replication/test_promotion.py -->
#### ISR Rejoin 回归测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

回归测试构造落后 HW 的 Replica，尝试让它 Rejoin，再尝试 Promotion；旧谓词会让两步都成功。

##### 关键测试语句

```python
assert replica_set.high_watermark == 1
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

ISR Membership 是安全承诺而非新鲜度提示。Rejoin Replica 必须拥有完整 Commit Prefix，才能参与 Ack 或成为 Leader。

### 为什么需要这个机制

Follower 可能接近 Leader LEO 却仍落后于已提交 High Watermark，因此只看 LEO 的 Rejoin 会接纳不安全的选举候选。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Membership Refresh 同时比较 Follower LEO、Lag Policy 与 HW；只有到达 HW 的 Replica 才能重新进入 Ack 与 Election 集合。

### 机制板块

<!-- journey-file: src/minikafka/replication/replica_set.py -->
#### ISR Rejoin 回归机制

##### 是什么，为什么现在需要

ISR Membership 是安全承诺而非新鲜度提示。Rejoin Replica 必须拥有完整 Commit Prefix，才能参与 Ack 或成为 Leader。

##### 在运行时做什么

Membership Refresh 同时比较 Follower LEO、Lag Policy 与 HW；只有到达 HW 的 Replica 才能重新进入 Ack 与 Election 集合。

##### 关键语句理解

`leo >= high_watermark` 这个合取条件是缺失的安全门：没有它，Leader Progress 变化会让不完整 Replica 看似已追上。



### 验证证据

运行 `uv run pytest -q $(cat journey/stages/19-isr-regression/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

`leo >= high_watermark` 这个合取条件是缺失的安全门：没有它，Leader Progress 变化会让不完整 Replica 看似已追上。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 6 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/06-isr-and-fencing.md)
