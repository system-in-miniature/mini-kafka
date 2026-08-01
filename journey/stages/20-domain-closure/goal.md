# Stage 20 · Cross-mechanism domain closure / 跨机制领域闭环

<!-- journey: chapter=9 tests_added=5 -->

## English

### Goal

Build cross-mechanism domain closure and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `pyproject.toml`
- `src/minikafka/__init__.py`
- `src/minikafka/consumer/group.py`
- `src/minikafka/core/cluster.py`
- `src/minikafka/labs/leader_failure.py`
- `src/minikafka/labs/rebalance.py`
- `src/minikafka/log/compaction.py`
- `src/minikafka/log/partition_log.py`
- `src/minikafka/log/retention.py`
- `src/minikafka/producer/state.py`
- `src/minikafka/replication/replica_set.py`
- `src/minikafka/transaction/manager.py`
- `tests/log/test_retention.py`
- `tests/test_final_acceptance.py`
- `tests/test_sloc_report.py`
- `tests/transaction/test_abort.py`
- `tests/transaction/test_visibility.py`
- `uv.lock`

### The problem at this point

Individually correct features can still violate one another when transactions, retention, replication visibility, restart, and the public API meet.

### Test contract

#### See the failure first

The final contracts combine rebalance, failover, restart, prefix retention, abort visibility, and prepared recovery; each test exposes a bug that isolated happy paths miss.

<!-- journey-file: tests/log/test_retention.py -->
<!-- journey-file: tests/test_final_acceptance.py -->
<!-- journey-file: tests/test_sloc_report.py -->
<!-- journey-file: tests/transaction/test_abort.py -->
<!-- journey-file: tests/transaction/test_visibility.py -->
#### Cross-mechanism domain closure test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

The final contracts combine rebalance, failover, restart, prefix retention, abort visibility, and prepared recovery; each test exposes a bug that isolated happy paths miss.

##### Key test statement

```python
assert len(log.closed_segments) >= 3
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

Domain closure means shared invariants survive composition. Transactional writes require `acks=all`; retention deletes only prefixes; `read_committed` uses replica-level transaction knowledge; public exports name the supported semantic surface.

### Why this mechanism is necessary

Individually correct features can still violate one another when transactions, retention, replication visibility, restart, and the public API meet. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

The cluster routes transactional appends through replicated acknowledgement, replica reads consult commit markers, retention advances one log-start boundary, and restart restores metadata, offsets, producer, and transaction state.

### Mechanism blocks

<!-- journey-file: src/minikafka/consumer/group.py -->
<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/labs/leader_failure.py -->
<!-- journey-file: src/minikafka/labs/rebalance.py -->
<!-- journey-file: src/minikafka/log/compaction.py -->
<!-- journey-file: src/minikafka/log/partition_log.py -->
<!-- journey-file: src/minikafka/log/retention.py -->
<!-- journey-file: src/minikafka/producer/state.py -->
<!-- journey-file: src/minikafka/replication/replica_set.py -->
<!-- journey-file: src/minikafka/transaction/manager.py -->
#### Cross-mechanism domain closure mechanism

##### What it is and why it appears

Domain closure means shared invariants survive composition. Transactional writes require `acks=all`; retention deletes only prefixes; `read_committed` uses replica-level transaction knowledge; public exports name the supported semantic surface.

##### Runtime role

The cluster routes transactional appends through replicated acknowledgement, replica reads consult commit markers, retention advances one log-start boundary, and restart restores metadata, offsets, producer, and transaction state.

##### Statement understanding

The final integration checks are not new features: they prove that local invariants use the same authority and durability boundaries when composed.

<!-- journey-file: pyproject.toml -->
<!-- journey-file: src/minikafka/__init__.py -->
<!-- journey-file: uv.lock -->
#### Package and project support

These files only keep the package boundary, dependencies, and test environment reproducible; they are supporting wiring rather than Kafka mechanism logic.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/20-domain-closure/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

The final integration checks are not new features: they prove that local invariants use the same authority and durability boundaries when composed.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 9](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/09-delivery-semantics.md)

## 中文

### 目标

实现跨机制领域闭环，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `pyproject.toml`
- `src/minikafka/__init__.py`
- `src/minikafka/consumer/group.py`
- `src/minikafka/core/cluster.py`
- `src/minikafka/labs/leader_failure.py`
- `src/minikafka/labs/rebalance.py`
- `src/minikafka/log/compaction.py`
- `src/minikafka/log/partition_log.py`
- `src/minikafka/log/retention.py`
- `src/minikafka/producer/state.py`
- `src/minikafka/replication/replica_set.py`
- `src/minikafka/transaction/manager.py`
- `tests/log/test_retention.py`
- `tests/test_final_acceptance.py`
- `tests/test_sloc_report.py`
- `tests/transaction/test_abort.py`
- `tests/transaction/test_visibility.py`
- `uv.lock`

### 当前遇到的问题

各自正确的功能在事务、Retention、复制可见性、重启与公共 API 交汇时仍可能互相破坏。

### 测试契约

#### 先看会坏在哪里

最终契约组合 Rebalance、Failover、Restart、Prefix Retention、Abort Visibility 与 Prepared Recovery；每条测试都暴露孤立 Happy Path 看不到的问题。

<!-- journey-file: tests/log/test_retention.py -->
<!-- journey-file: tests/test_final_acceptance.py -->
<!-- journey-file: tests/test_sloc_report.py -->
<!-- journey-file: tests/transaction/test_abort.py -->
<!-- journey-file: tests/transaction/test_visibility.py -->
#### 跨机制领域闭环测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

最终契约组合 Rebalance、Failover、Restart、Prefix Retention、Abort Visibility 与 Prepared Recovery；每条测试都暴露孤立 Happy Path 看不到的问题。

##### 关键测试语句

```python
assert len(log.closed_segments) >= 3
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

领域闭环意味着共享不变量在组合后仍成立。事务写要求 `acks=all`；Retention 只删前缀；`read_committed` 使用 Replica 级事务知识；公共导出明确支持的语义表面。

### 为什么需要这个机制

各自正确的功能在事务、Retention、复制可见性、重启与公共 API 交汇时仍可能互相破坏。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Cluster 让事务 Append 经过复制确认；Replica Read 查询 Commit Marker；Retention 推进唯一 Log Start Boundary；Restart 恢复 Metadata、Offset、Producer 与 Transaction State。

### 机制板块

<!-- journey-file: src/minikafka/consumer/group.py -->
<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/labs/leader_failure.py -->
<!-- journey-file: src/minikafka/labs/rebalance.py -->
<!-- journey-file: src/minikafka/log/compaction.py -->
<!-- journey-file: src/minikafka/log/partition_log.py -->
<!-- journey-file: src/minikafka/log/retention.py -->
<!-- journey-file: src/minikafka/producer/state.py -->
<!-- journey-file: src/minikafka/replication/replica_set.py -->
<!-- journey-file: src/minikafka/transaction/manager.py -->
#### 跨机制领域闭环机制

##### 是什么，为什么现在需要

领域闭环意味着共享不变量在组合后仍成立。事务写要求 `acks=all`；Retention 只删前缀；`read_committed` 使用 Replica 级事务知识；公共导出明确支持的语义表面。

##### 在运行时做什么

Cluster 让事务 Append 经过复制确认；Replica Read 查询 Commit Marker；Retention 推进唯一 Log Start Boundary；Restart 恢复 Metadata、Offset、Producer 与 Transaction State。

##### 关键语句理解

最终集成检查不是新功能；它证明局部不变量组合时仍使用同一套权威与持久性边界。

<!-- journey-file: pyproject.toml -->
<!-- journey-file: src/minikafka/__init__.py -->
<!-- journey-file: uv.lock -->
#### 包与工程支撑

这些文件只负责让本 Stage 的包边界、依赖与测试环境可复现，不把脚手架误讲成 Kafka 机制。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/20-domain-closure/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

最终集成检查不是新功能；它证明局部不变量组合时仍使用同一套权威与持久性边界。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 9 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/09-delivery-semantics.md)
