# Stage 02 · Binary-safe record batches / 二进制安全 Record Batch

<!-- journey: chapter=2 tests_added=1 -->

## English

### Goal

Build binary-safe record batches and explain its boundary from executable failure, runtime state, and the critical statement.

### Deliverable files

- `src/minikafka/core/__init__.py`
- `src/minikafka/core/batch.py`
- `src/minikafka/core/batch_codec.py`
- `src/minikafka/core/record.py`
- `src/minikafka/errors.py`
- `tests/unit/test_batch_codec.py`

### The problem at this point

A broker needs one durable frame that preserves arbitrary bytes and can detect corruption before records enter the log.

### Test contract

#### See the failure first

Round-trip tests include binary keys, values, and headers; corruption and truncation tests prove a decoder must reject plausible-looking partial data.

<!-- journey-file: tests/unit/test_batch_codec.py -->
#### Binary-safe record batches test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

Round-trip tests include binary keys, values, and headers; corruption and truncation tests prove a decoder must reject plausible-looking partial data.

##### Key test statement

```python
assert decoded == batch
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

A Record is user data; a RecordBatch is the append and replication unit; framing states lengths explicitly; CRC authenticates the encoded payload rather than Python objects.

### Why this mechanism is necessary

A broker needs one durable frame that preserves arbitrary bytes and can detect corruption before records enter the log. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Encoding assigns a stable binary layout and checksum; decoding validates size, version, control/data invariants, and CRC before constructing domain objects.

### Mechanism blocks

<!-- journey-file: src/minikafka/core/batch.py -->
<!-- journey-file: src/minikafka/core/batch_codec.py -->
<!-- journey-file: src/minikafka/core/record.py -->
<!-- journey-file: src/minikafka/errors.py -->
#### Binary-safe record batches mechanism

##### What it is and why it appears

A Record is user data; a RecordBatch is the append and replication unit; framing states lengths explicitly; CRC authenticates the encoded payload rather than Python objects.

##### Runtime role

Encoding assigns a stable binary layout and checksum; decoding validates size, version, control/data invariants, and CRC before constructing domain objects.

##### Statement understanding

Length checks establish safe read boundaries first; only then may the CRC and semantic fields be trusted.

<!-- journey-file: src/minikafka/core/__init__.py -->
#### Package and project support

These files only keep the package boundary, dependencies, and test environment reproducible; they are supporting wiring rather than Kafka mechanism logic.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/02-record-batches/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Length checks establish safe read boundaries first; only then may the CRC and semantic fields be trusted.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/02-the-log.md)

## 中文

### 目标

实现二进制安全 Record Batch，并能从可执行失败、运行时状态与关键语句解释其边界。

### 交付文件

- `src/minikafka/core/__init__.py`
- `src/minikafka/core/batch.py`
- `src/minikafka/core/batch_codec.py`
- `src/minikafka/core/record.py`
- `src/minikafka/errors.py`
- `tests/unit/test_batch_codec.py`

### 当前遇到的问题

Broker 需要一种既能保存任意字节、又能在 Record 进入日志前发现损坏的持久帧。

### 测试契约

#### 先看会坏在哪里

Round-trip 测试包含二进制 Key、Value 与 Header；损坏和截断测试证明 Decoder 必须拒绝看似合理的残缺数据。

<!-- journey-file: tests/unit/test_batch_codec.py -->
#### 二进制安全 Record Batch测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

Round-trip 测试包含二进制 Key、Value 与 Header；损坏和截断测试证明 Decoder 必须拒绝看似合理的残缺数据。

##### 关键测试语句

```python
assert decoded == batch
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Record 是用户数据；RecordBatch 是追加与复制单元；Framing 显式记录长度；CRC 校验编码后的 Payload 而非 Python 对象。

### 为什么需要这个机制

Broker 需要一种既能保存任意字节、又能在 Record 进入日志前发现损坏的持久帧。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

编码建立稳定二进制布局和校验和；解码在构造领域对象前验证长度、版本、控制/数据约束与 CRC。

### 机制板块

<!-- journey-file: src/minikafka/core/batch.py -->
<!-- journey-file: src/minikafka/core/batch_codec.py -->
<!-- journey-file: src/minikafka/core/record.py -->
<!-- journey-file: src/minikafka/errors.py -->
#### 二进制安全 Record Batch机制

##### 是什么，为什么现在需要

Record 是用户数据；RecordBatch 是追加与复制单元；Framing 显式记录长度；CRC 校验编码后的 Payload 而非 Python 对象。

##### 在运行时做什么

编码建立稳定二进制布局和校验和；解码在构造领域对象前验证长度、版本、控制/数据约束与 CRC。

##### 关键语句理解

长度检查先建立安全读取边界；之后 CRC 与语义字段才值得信任。

<!-- journey-file: src/minikafka/core/__init__.py -->
#### 包与工程支撑

这些文件只负责让本 Stage 的包边界、依赖与测试环境可复现，不把脚手架误讲成 Kafka 机制。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/02-record-batches/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

长度检查先建立安全读取边界；之后 CRC 与语义字段才值得信任。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/02-the-log.md)
