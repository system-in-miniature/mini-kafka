# Stage 17 · Thin JSON TCP adapter / 轻量 JSON TCP Adapter

<!-- journey: chapter=10 tests_added=2 -->

## English

### Goal

Build thin json tcp adapter and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/adapters/json_tcp.py`
- `src/minikafka/errors.py`
- `tests/adapters/test_direct_tcp_parity.py`
- `tests/adapters/test_json_tcp.py`

### The problem at this point

A network API is useful only if framing and translation do not create a second, divergent broker semantics.

### Test contract

#### See the failure first

Parity tests execute equivalent Direct and TCP operations; framing tests split stream input, carry binary values, and require typed domain errors in JSON responses.

<!-- journey-file: tests/adapters/test_direct_tcp_parity.py -->
<!-- journey-file: tests/adapters/test_json_tcp.py -->
#### Thin JSON TCP adapter test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Parity tests execute equivalent Direct and TCP operations; framing tests split stream input, carry binary values, and require typed domain errors in JSON responses.

##### Key test statement

```python
assert json.loads(await reader.readline())["ok"] is True
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

The adapter uses a length-prefixed JSON envelope. Binary fields require explicit encoding. Dispatch translates requests to the Direct semantic core and maps domain failures back to stable wire errors.

### Why this mechanism is necessary

A network API is useful only if framing and translation do not create a second, divergent broker semantics. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

The server reads an exact frame length, validates request shape, calls existing cluster operations, and serializes results without owning storage or replication decisions.

### Mechanism blocks

<!-- journey-file: src/minikafka/adapters/json_tcp.py -->
<!-- journey-file: src/minikafka/errors.py -->
#### Thin JSON TCP adapter mechanism

##### What it is and why it appears

The adapter uses a length-prefixed JSON envelope. Binary fields require explicit encoding. Dispatch translates requests to the Direct semantic core and maps domain failures back to stable wire errors.

##### Runtime role

The server reads an exact frame length, validates request shape, calls existing cluster operations, and serializes results without owning storage or replication decisions.

##### Statement understanding

A transport handler may translate types but must not reimplement acknowledgement, visibility, or offset rules; parity evidence protects that boundary.



### Verification evidence

Run `uv run pytest -q $(cat journey/stages/17-json-tcp/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

A transport handler may translate types but must not reimplement acknowledgement, visibility, or offset rules; parity evidence protects that boundary.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 10](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/10-protocol-and-beyond.md)

## 中文

### 目标

实现轻量 JSON TCP Adapter，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/adapters/json_tcp.py`
- `src/minikafka/errors.py`
- `tests/adapters/test_direct_tcp_parity.py`
- `tests/adapters/test_json_tcp.py`

### 当前遇到的问题

网络 API 只有在 Framing 与 Translation 不制造第二套分叉 Broker 语义时才有价值。

### 测试契约

#### 先看会坏在哪里

Parity 测试执行等价 Direct 与 TCP 操作；Framing 测试拆分 Stream 输入、携带二进制值，并要求 JSON Response 返回类型化领域错误。

<!-- journey-file: tests/adapters/test_direct_tcp_parity.py -->
<!-- journey-file: tests/adapters/test_json_tcp.py -->
#### 轻量 JSON TCP Adapter测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

Parity 测试执行等价 Direct 与 TCP 操作；Framing 测试拆分 Stream 输入、携带二进制值，并要求 JSON Response 返回类型化领域错误。

##### 关键测试语句

```python
assert json.loads(await reader.readline())["ok"] is True
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Adapter 使用长度前缀 JSON Envelope；二进制字段需要显式编码；Dispatch 把请求翻译到 Direct 语义核心，并把领域失败映射回稳定 Wire Error。

### 为什么需要这个机制

网络 API 只有在 Framing 与 Translation 不制造第二套分叉 Broker 语义时才有价值。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Server 精确读取 Frame 长度、校验 Request Shape、调用已有 Cluster Operation，再序列化结果；它不拥有存储或复制决策。

### 机制板块

<!-- journey-file: src/minikafka/adapters/json_tcp.py -->
<!-- journey-file: src/minikafka/errors.py -->
#### 轻量 JSON TCP Adapter机制

##### 是什么，为什么现在需要

Adapter 使用长度前缀 JSON Envelope；二进制字段需要显式编码；Dispatch 把请求翻译到 Direct 语义核心，并把领域失败映射回稳定 Wire Error。

##### 在运行时做什么

Server 精确读取 Frame 长度、校验 Request Shape、调用已有 Cluster Operation，再序列化结果；它不拥有存储或复制决策。

##### 关键语句理解

Transport Handler 可以翻译类型，但不得重写 Ack、Visibility 或 Offset 规则；Parity Evidence 保护这条边界。



### 验证证据

运行 `uv run pytest -q $(cat journey/stages/17-json-tcp/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

Transport Handler 可以翻译类型，但不得重写 Ack、Visibility 或 Offset 规则；Parity Evidence 保护这条边界。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 10 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/10-protocol-and-beyond.md)
