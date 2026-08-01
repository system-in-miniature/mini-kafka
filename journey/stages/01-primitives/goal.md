# Stage 01 · Deterministic foundations / 确定性基础

<!-- journey: chapter=1 tests_added=1 -->

## English

### Goal

Build deterministic foundations and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `pyproject.toml`
- `src/minikafka/__init__.py`
- `src/minikafka/clock.py`
- `src/minikafka/config.py`
- `src/minikafka/errors.py`
- `tests/unit/test_primitives.py`
- `uv.lock`

### The problem at this point

Storage and coordination logic cannot be reproduced if time, configuration, and failures have unstable meanings.

### Test contract

#### See the failure first

A clock that moves backwards or a configuration that accepts an impossible segment size lets later state machines start from invalid facts.

<!-- journey-file: tests/unit/test_primitives.py -->
#### Deterministic foundations test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

A clock that moves backwards or a configuration that accepts an impossible segment size lets later state machines start from invalid facts.

##### Key test statement

```python
assert clock.now_ms() == 125
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

A protocol clock is an injected source of time; validated configuration is an executable boundary; typed errors make failure classes observable without parsing messages.

### Why this mechanism is necessary

Storage and coordination logic cannot be reproduced if time, configuration, and failures have unstable meanings. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Callers read time through the Clock protocol, tests advance ManualClock explicitly, and configuration rejects invalid values before any log is opened.

### Mechanism blocks

<!-- journey-file: src/minikafka/clock.py -->
<!-- journey-file: src/minikafka/config.py -->
<!-- journey-file: src/minikafka/errors.py -->
#### Deterministic foundations mechanism

##### What it is and why it appears

A protocol clock is an injected source of time; validated configuration is an executable boundary; typed errors make failure classes observable without parsing messages.

##### Runtime role

Callers read time through the Clock protocol, tests advance ManualClock explicitly, and configuration rejects invalid values before any log is opened.

##### Statement understanding

The non-negative advance guard preserves monotonic time, while stable error codes preserve machine-readable failure identity.

<!-- journey-file: pyproject.toml -->
<!-- journey-file: src/minikafka/__init__.py -->
<!-- journey-file: uv.lock -->
#### Package and project support

These files only keep the package boundary, dependencies, and test environment reproducible; they are supporting wiring rather than Kafka mechanism logic.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/01-primitives/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

The non-negative advance guard preserves monotonic time, while stable error codes preserve machine-readable failure identity.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 1](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/01-getting-started.md)

## 中文

### 目标

实现确定性基础，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `pyproject.toml`
- `src/minikafka/__init__.py`
- `src/minikafka/clock.py`
- `src/minikafka/config.py`
- `src/minikafka/errors.py`
- `tests/unit/test_primitives.py`
- `uv.lock`

### 当前遇到的问题

如果时间、配置与失败没有稳定语义，存储和协调逻辑就无法复现。

### 测试契约

#### 先看会坏在哪里

允许时钟倒退或接受不可能 Segment 大小的配置，会让后续状态机从错误事实起步。

<!-- journey-file: tests/unit/test_primitives.py -->
#### 确定性基础测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

允许时钟倒退或接受不可能 Segment 大小的配置，会让后续状态机从错误事实起步。

##### 关键测试语句

```python
assert clock.now_ms() == 125
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

协议时钟是注入的时间来源；经过校验的配置是可执行边界；类型化错误让调用方无需解析文本即可识别失败类别。

### 为什么需要这个机制

如果时间、配置与失败没有稳定语义，存储和协调逻辑就无法复现。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

调用方只通过 Clock 协议读取时间，测试显式推进 ManualClock，配置在日志打开前拒绝非法值。

### 机制板块

<!-- journey-file: src/minikafka/clock.py -->
<!-- journey-file: src/minikafka/config.py -->
<!-- journey-file: src/minikafka/errors.py -->
#### 确定性基础机制

##### 是什么，为什么现在需要

协议时钟是注入的时间来源；经过校验的配置是可执行边界；类型化错误让调用方无需解析文本即可识别失败类别。

##### 在运行时做什么

调用方只通过 Clock 协议读取时间，测试显式推进 ManualClock，配置在日志打开前拒绝非法值。

##### 关键语句理解

非负推进检查保持时间单调；稳定错误码保持机器可读的失败身份。

<!-- journey-file: pyproject.toml -->
<!-- journey-file: src/minikafka/__init__.py -->
<!-- journey-file: uv.lock -->
#### 包与工程支撑

这些文件只负责让本 Stage 的包边界、依赖与测试环境可复现，不把脚手架误讲成 Kafka 机制。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/01-primitives/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

非负推进检查保持时间单调；稳定错误码保持机器可读的失败身份。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 1 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/01-getting-started.md)
