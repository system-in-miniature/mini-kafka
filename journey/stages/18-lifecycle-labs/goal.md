# Stage 18 · Lifecycle and failure labs / 生命周期与失败实验

<!-- journey: chapter=9 tests_added=2 -->

## English

### Goal

Build lifecycle and failure labs and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/core/cluster.py`
- `src/minikafka/labs/__init__.py`
- `src/minikafka/labs/leader_failure.py`
- `src/minikafka/labs/rebalance.py`
- `src/minikafka/lifecycle.py`
- `src/minikafka/producer/producer.py`
- `tests/reliability/test_background_failure.py`
- `tests/reliability/test_shutdown.py`

### The problem at this point

Background tasks can fail after an API call returns, and an unowned shutdown can leave tasks, sockets, or buffered records behind.

### Test contract

#### See the failure first

Failure tests inject a background exception and require the next boundary to surface it; shutdown tests verify flush, task cancellation, and idempotent close.

<!-- journey-file: tests/reliability/test_background_failure.py -->
<!-- journey-file: tests/reliability/test_shutdown.py -->
#### Lifecycle and failure labs test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Failure tests inject a background exception and require the next boundary to surface it; shutdown tests verify flush, task cancellation, and idempotent close.

##### Key test statement

```python
assert cluster.state is LifecycleState.FAILED
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

Lifecycle ownership means one component starts, observes, and closes every resource it creates. A failure injector makes asynchronous faults deterministic. Labs are runnable evidence, not alternate implementations.

### Why this mechanism is necessary

Background tasks can fail after an API call returns, and an unowned shutdown can leave tasks, sockets, or buffered records behind. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Cluster and producer close paths stop new work, flush pending records, await or cancel owned tasks, surface captured failures, and tolerate repeated close calls.

### Mechanism blocks

<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/labs/leader_failure.py -->
<!-- journey-file: src/minikafka/labs/rebalance.py -->
<!-- journey-file: src/minikafka/lifecycle.py -->
<!-- journey-file: src/minikafka/producer/producer.py -->
#### Lifecycle and failure labs mechanism

##### What it is and why it appears

Lifecycle ownership means one component starts, observes, and closes every resource it creates. A failure injector makes asynchronous faults deterministic. Labs are runnable evidence, not alternate implementations.

##### Runtime role

Cluster and producer close paths stop new work, flush pending records, await or cancel owned tasks, surface captured failures, and tolerate repeated close calls.

##### Statement understanding

A background exception must cross an owned API boundary; logging it without changing observable state would turn data loss into apparent success.

<!-- journey-file: src/minikafka/labs/__init__.py -->
#### Package and project support

These files only keep the package boundary, dependencies, and test environment reproducible; they are supporting wiring rather than Kafka mechanism logic.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/18-lifecycle-labs/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

A background exception must cross an owned API boundary; logging it without changing observable state would turn data loss into apparent success.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 9](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/09-delivery-semantics.md)

## 中文

### 目标

实现生命周期与失败实验，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/core/cluster.py`
- `src/minikafka/labs/__init__.py`
- `src/minikafka/labs/leader_failure.py`
- `src/minikafka/labs/rebalance.py`
- `src/minikafka/lifecycle.py`
- `src/minikafka/producer/producer.py`
- `tests/reliability/test_background_failure.py`
- `tests/reliability/test_shutdown.py`

### 当前遇到的问题

后台任务可能在 API 返回后失败；没有统一所有权的 Shutdown 会遗留 Task、Socket 或 Buffered Record。

### 测试契约

#### 先看会坏在哪里

Failure 测试注入后台异常并要求下一个边界暴露它；Shutdown 测试验证 Flush、Task Cancellation 与幂等 Close。

<!-- journey-file: tests/reliability/test_background_failure.py -->
<!-- journey-file: tests/reliability/test_shutdown.py -->
#### 生命周期与失败实验测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

Failure 测试注入后台异常并要求下一个边界暴露它；Shutdown 测试验证 Flush、Task Cancellation 与幂等 Close。

##### 关键测试语句

```python
assert cluster.state is LifecycleState.FAILED
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

生命周期所有权意味着组件负责启动、观测并关闭自己创建的每个资源；Failure Injector 让异步故障确定化；Lab 是可运行证据而非另一套实现。

### 为什么需要这个机制

后台任务可能在 API 返回后失败；没有统一所有权的 Shutdown 会遗留 Task、Socket 或 Buffered Record。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Cluster 与 Producer 的 Close Path 停止新工作、Flush Pending Record、等待或取消自有 Task、暴露捕获失败，并允许重复 Close。

### 机制板块

<!-- journey-file: src/minikafka/core/cluster.py -->
<!-- journey-file: src/minikafka/labs/leader_failure.py -->
<!-- journey-file: src/minikafka/labs/rebalance.py -->
<!-- journey-file: src/minikafka/lifecycle.py -->
<!-- journey-file: src/minikafka/producer/producer.py -->
#### 生命周期与失败实验机制

##### 是什么，为什么现在需要

生命周期所有权意味着组件负责启动、观测并关闭自己创建的每个资源；Failure Injector 让异步故障确定化；Lab 是可运行证据而非另一套实现。

##### 在运行时做什么

Cluster 与 Producer 的 Close Path 停止新工作、Flush Pending Record、等待或取消自有 Task、暴露捕获失败，并允许重复 Close。

##### 关键语句理解

后台异常必须穿过一个有所有权的 API Boundary；只记录日志却不改变可观察状态，会把数据丢失伪装成成功。

<!-- journey-file: src/minikafka/labs/__init__.py -->
#### 包与工程支撑

这些文件只负责让本 Stage 的包边界、依赖与测试环境可复现，不把脚手架误讲成 Kafka 机制。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/18-lifecycle-labs/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

后台异常必须穿过一个有所有权的 API Boundary；只记录日志却不改变可观察状态，会把数据丢失伪装成成功。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 9 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/09-delivery-semantics.md)
