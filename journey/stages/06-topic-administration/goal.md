# Stage 06 · Topics and direct administration / Topic 与直接管理接口

<!-- journey: chapter=1 tests_added=2 -->

## English

### Goal

Build topics and direct administration and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/adapters/__init__.py`
- `src/minikafka/adapters/direct.py`
- `src/minikafka/config.py`
- `src/minikafka/core/cluster.py`
- `src/minikafka/core/metadata.py`
- `src/minikafka/errors.py`
- `tests/test_direct_cluster.py`
- `tests/unit/test_metadata.py`

### The problem at this point

Durable logs are not yet a broker: callers need named topics, partition metadata, replica placement, and one semantic API.

### Test contract

#### See the failure first

Administration tests reject invalid names and replication factors, then reopen the cluster to prove metadata and log directories agree.

<!-- journey-file: tests/test_direct_cluster.py -->
<!-- journey-file: tests/unit/test_metadata.py -->
#### Topics and direct administration test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Administration tests reject invalid names and replication factors, then reopen the cluster to prove metadata and log directories agree.

##### Key test statement

```python
assert tuple(topic.partitions) == (0, 1, 2)
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

TopicPartition is the stable address of one ordered log. Metadata records leader and replicas. The Direct API is the semantic reference rather than a transport protocol.

### Why this mechanism is necessary

Durable logs are not yet a broker: callers need named topics, partition metadata, replica placement, and one semantic API. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Create-topic validates the request, assigns replicas deterministically, creates partition logs, persists metadata, and exposes the result through DirectAdmin.

### Mechanism blocks

<!-- journey-file: src/minikafka/adapters/direct.py -->
<!-- journey-file: src/minikafka/config.py -->
<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/core/metadata.py -->
<!-- journey-file: src/minikafka/errors.py -->
#### Topics and direct administration mechanism

##### What it is and why it appears

TopicPartition is the stable address of one ordered log. Metadata records leader and replicas. The Direct API is the semantic reference rather than a transport protocol.

##### Runtime role

Create-topic validates the request, assigns replicas deterministically, creates partition logs, persists metadata, and exposes the result through DirectAdmin.

##### Statement understanding

Metadata must be persisted with the same partition identity used to locate logs; otherwise restart reconstructs a different broker topology.

<!-- journey-file: src/minikafka/adapters/__init__.py -->
#### Package and project support

These files only keep the package boundary, dependencies, and test environment reproducible; they are supporting wiring rather than Kafka mechanism logic.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/06-topic-administration/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Metadata must be persisted with the same partition identity used to locate logs; otherwise restart reconstructs a different broker topology.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 1](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/01-getting-started.md)

## 中文

### 目标

实现Topic 与直接管理接口，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/adapters/__init__.py`
- `src/minikafka/adapters/direct.py`
- `src/minikafka/config.py`
- `src/minikafka/core/cluster.py`
- `src/minikafka/core/metadata.py`
- `src/minikafka/errors.py`
- `tests/test_direct_cluster.py`
- `tests/unit/test_metadata.py`

### 当前遇到的问题

持久日志还不是 Broker：调用方需要具名 Topic、Partition Metadata、Replica Placement 与统一语义 API。

### 测试契约

#### 先看会坏在哪里

管理测试拒绝非法名称与复制因子，并重开 Cluster 证明 Metadata 与日志目录一致。

<!-- journey-file: tests/test_direct_cluster.py -->
<!-- journey-file: tests/unit/test_metadata.py -->
#### Topic 与直接管理接口测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

管理测试拒绝非法名称与复制因子，并重开 Cluster 证明 Metadata 与日志目录一致。

##### 关键测试语句

```python
assert tuple(topic.partitions) == (0, 1, 2)
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

TopicPartition 是单条有序日志的稳定地址。Metadata 记录 Leader 与 Replica。Direct API 是语义参考而非传输协议。

### 为什么需要这个机制

持久日志还不是 Broker：调用方需要具名 Topic、Partition Metadata、Replica Placement 与统一语义 API。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Create Topic 校验请求、确定性分配 Replica、创建 Partition Log、持久化 Metadata，并通过 DirectAdmin 暴露结果。

### 机制板块

<!-- journey-file: src/minikafka/adapters/direct.py -->
<!-- journey-file: src/minikafka/config.py -->
<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/core/metadata.py -->
<!-- journey-file: src/minikafka/errors.py -->
#### Topic 与直接管理接口机制

##### 是什么，为什么现在需要

TopicPartition 是单条有序日志的稳定地址。Metadata 记录 Leader 与 Replica。Direct API 是语义参考而非传输协议。

##### 在运行时做什么

Create Topic 校验请求、确定性分配 Replica、创建 Partition Log、持久化 Metadata，并通过 DirectAdmin 暴露结果。

##### 关键语句理解

Metadata 必须用与日志定位相同的 Partition 身份持久化，否则重启会重建出不同拓扑。

<!-- journey-file: src/minikafka/adapters/__init__.py -->
#### 包与工程支撑

这些文件只负责让本 Stage 的包边界、依赖与测试环境可复现，不把脚手架误讲成 Kafka 机制。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/06-topic-administration/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

Metadata 必须用与日志定位相同的 Partition 身份持久化，否则重启会重建出不同拓扑。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 1 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/01-getting-started.md)
