# Stage 18 · 生命周期与失败实验

### 目标

实现生命周期与失败实验，并能从可执行失败、运行时状态与关键语句解释其边界。

??? note "交付文件"
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

??? note "文件差异：tests/reliability/test_background_failure.py"
    ```diff
    diff --git a/tests/reliability/test_background_failure.py b/tests/reliability/test_background_failure.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..335612862ea413157c5674aafe30a47960f8899d
    --- /dev/null
    +++ b/tests/reliability/test_background_failure.py
    @@ -0,0 +1,27 @@
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.errors import StorageError
    +from minikafka.lifecycle import LifecycleState
    +
    +
    +@pytest.mark.asyncio
    +async def test_background_storage_failure_is_terminal(tmp_path: Path) -> None:
    +    cluster = BrokerCluster.open(
    +        MiniKafkaConfig(data_dir=tmp_path),
    +        clock=ManualClock(),
    +    )
    +    failure = StorageError("disk failed")
    +    cluster.failure_injector.fail_next_flush(failure)
    +
    +    with pytest.raises(StorageError):
    +        await cluster.run_flush_cycle()
    +
    +    assert cluster.state is LifecycleState.FAILED
    +    with pytest.raises(StorageError, match="disk failed"):
    +        await cluster.create_topic("later", 1, 1)
    +    await cluster.crash()
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

Failure 测试注入后台异常并要求下一个边界暴露它；Shutdown 测试验证 Flush、Task Cancellation 与幂等 Close。

**关键测试语句**

```python
assert cluster.state is LifecycleState.FAILED
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

??? note "文件差异：tests/reliability/test_shutdown.py"
    ```diff
    diff --git a/tests/reliability/test_shutdown.py b/tests/reliability/test_shutdown.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..947b347ef56e9a5ccd4cb8e794f50b92b0492b77
    --- /dev/null
    +++ b/tests/reliability/test_shutdown.py
    @@ -0,0 +1,56 @@
    +import asyncio
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.lifecycle import LifecycleState
    +
    +
    +@pytest.mark.asyncio
    +async def test_graceful_close_drains_producer_and_is_idempotent(
    +    tmp_path: Path,
    +) -> None:
    +    cluster = BrokerCluster.open(
    +        MiniKafkaConfig(data_dir=tmp_path),
    +        clock=ManualClock(),
    +    )
    +    await cluster.create_topic("events", 1, 1)
    +    producer = cluster.producer(linger_ms=1_000)
    +    pending = producer.send("events", value=b"flush-me")
    +
    +    await cluster.close()
    +    await cluster.close()
    +
    +    assert (await pending).offset == 0
    +    assert cluster.state is LifecycleState.CLOSED
    +    assert cluster.owned_tasks == ()
    +
    +    reopened = BrokerCluster.open(
    +        MiniKafkaConfig(data_dir=tmp_path),
    +        clock=ManualClock(),
    +    )
    +    assert reopened.leader_log(TopicPartition("events", 0)).leo == 1
    +    await reopened.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_crash_does_not_drain_buffered_producer(
    +    tmp_path: Path,
    +) -> None:
    +    cluster = BrokerCluster.open(
    +        MiniKafkaConfig(data_dir=tmp_path),
    +        clock=ManualClock(),
    +    )
    +    await cluster.create_topic("events", 1, 1)
    +    producer = cluster.producer(linger_ms=1_000)
    +    pending = producer.send("events", value=b"lost")
    +
    +    await cluster.crash()
    +
    +    assert pending.cancelled()
    +    assert cluster.state is LifecycleState.CLOSED
    +    await asyncio.sleep(0)
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

Failure 测试注入后台异常并要求下一个边界暴露它；Shutdown 测试验证 Flush、Task Cancellation 与幂等 Close。

**关键测试语句**

```python
assert cluster.state is LifecycleState.FAILED
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

生命周期所有权意味着组件负责启动、观测并关闭自己创建的每个资源；Failure Injector 让异步故障确定化；Lab 是可运行证据而非另一套实现。

### 为什么需要这个机制

后台任务可能在 API 返回后失败；没有统一所有权的 Shutdown 会遗留 Task、Socket 或 Buffered Record。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Cluster 与 Producer 的 Close Path 停止新工作、Flush Pending Record、等待或取消自有 Task、暴露捕获失败，并允许重复 Close。

### 机制板块

#### 生命周期与失败实验机制

Cluster 与 Producer 的 Close Path 停止新工作、Flush Pending Record、等待或取消自有 Task、暴露捕获失败，并允许重复 Close。

??? note "文件差异：src/minikafka/core/cluster.py"
    ```diff
    diff --git a/src/minikafka/core/cluster.py b/src/minikafka/core/cluster.py
    index 7d3703a230291df393d1f5270a07704bcbdc87fa..9bf1810f9ccdbc7623c519169bcf5f5d45210967 100644
    --- a/src/minikafka/core/cluster.py
    +++ b/src/minikafka/core/cluster.py
    @@ -23,6 +23,7 @@ from minikafka.errors import (
         UnknownPartition,
         UnknownTopic,
     )
    +from minikafka.lifecycle import FailureInjector, LifecycleState
     from minikafka.log.partition_log import PartitionLog
     from minikafka.log.segment import LocatedBatch
     from minikafka.producer.producer import Producer
    @@ -68,6 +69,9 @@ class BrokerCluster:
                 self,
                 TransactionJournal(config.data_dir / "transactions.journal"),
             )
    +        self.state = LifecycleState.RUNNING
    +        self.failure_injector = FailureInjector()
    +        self._terminal_error: BaseException | None = None
             self._closed = False

         @classmethod
    @@ -383,6 +387,7 @@ class BrokerCluster:
         async def close(self) -> None:
             if self._closed:
                 return
    +        self.state = LifecycleState.CLOSING
             for producer in tuple(self._producers):
                 await producer.close()
             for consumer in tuple(self._consumers):
    @@ -392,7 +397,42 @@ class BrokerCluster:
                 log.flush()
                 log.close()
             self._closed = True
    +        self.state = LifecycleState.CLOSED
    +
    +    async def crash(self) -> None:
    +        if self._closed:
    +            return
    +        for producer in tuple(self._producers):
    +            await producer.crash()
    +        for consumer in tuple(self._consumers):
    +            await consumer.close()
    +        for log in self._logs.values():
    +            log.close()
    +        self._closed = True
    +        self.state = LifecycleState.CLOSED
    +
    +    async def run_flush_cycle(self) -> None:
    +        self._ensure_open()
    +        try:
    +            self.failure_injector.before_flush()
    +            for log in self._logs.values():
    +                log.flush()
    +            self.offsets.flush()
    +        except BaseException as error:
    +            self._terminal_error = error
    +            self.state = LifecycleState.FAILED
    +            raise
    +
    +    @property
    +    def owned_tasks(self) -> tuple[object, ...]:
    +        return tuple(
    +            task
    +            for producer in self._producers
    +            for task in producer.owned_tasks
    +        )

         def _ensure_open(self) -> None:
    +        if self._terminal_error is not None:
    +            raise self._terminal_error
             if self._closed:
                 raise RuntimeError("cluster is closed")
    ```

??? note "文件差异：src/minikafka/labs/leader_failure.py"
    ```diff
    diff --git a/src/minikafka/labs/leader_failure.py b/src/minikafka/labs/leader_failure.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..9e45d53dcc5926c7dc6816eed57d0558f2625933
    --- /dev/null
    +++ b/src/minikafka/labs/leader_failure.py
    @@ -0,0 +1,15 @@
    +from __future__ import annotations
    +
    +
    +def acknowledged_write_observation(
    +    *,
    +    leader_leo: int,
    +    follower_leo: int,
    +    high_watermark: int,
    +) -> dict[str, int | bool]:
    +    return {
    +        "leader_leo": leader_leo,
    +        "follower_leo": follower_leo,
    +        "high_watermark": high_watermark,
    +        "leader_only_tail_at_risk": leader_leo > high_watermark,
    +    }
    ```

??? note "文件差异：src/minikafka/labs/rebalance.py"
    ```diff
    diff --git a/src/minikafka/labs/rebalance.py b/src/minikafka/labs/rebalance.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..43709a0d740d088b3c1690e2466a0a0837f9e825
    --- /dev/null
    +++ b/src/minikafka/labs/rebalance.py
    @@ -0,0 +1,12 @@
    +from __future__ import annotations
    +
    +
    +def rebalance_observation(
    +    old_generation: int,
    +    new_generation: int,
    +) -> dict[str, int | bool]:
    +    return {
    +        "old_generation": old_generation,
    +        "new_generation": new_generation,
    +        "old_member_fenced": new_generation > old_generation,
    +    }
    ```

??? note "文件差异：src/minikafka/lifecycle.py"
    ```diff
    diff --git a/src/minikafka/lifecycle.py b/src/minikafka/lifecycle.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..9c1188463574484bee073ee482e677e89dae2aa9
    --- /dev/null
    +++ b/src/minikafka/lifecycle.py
    @@ -0,0 +1,25 @@
    +from __future__ import annotations
    +
    +from enum import Enum
    +
    +
    +class LifecycleState(str, Enum):
    +    RUNNING = "running"
    +    CLOSING = "closing"
    +    CLOSED = "closed"
    +    FAILED = "failed"
    +
    +
    +class FailureInjector:
    +    def __init__(self) -> None:
    +        self._next_flush: BaseException | None = None
    +
    +    def fail_next_flush(self, error: BaseException) -> None:
    +        self._next_flush = error
    +
    +    def before_flush(self) -> None:
    +        if self._next_flush is None:
    +            return
    +        error = self._next_flush
    +        self._next_flush = None
    +        raise error
    ```

??? note "文件差异：src/minikafka/producer/producer.py"
    ```diff
    diff --git a/src/minikafka/producer/producer.py b/src/minikafka/producer/producer.py
    index 7c03cc74f81b8bc8f41907b0f723841e85dbd1e6..6f9c71eda0d0bd36bf0ed856d1e0a6326da5c7fc 100644
    --- a/src/minikafka/producer/producer.py
    +++ b/src/minikafka/producer/producer.py
    @@ -158,3 +158,21 @@ class Producer:
                 return
             await self.flush()
             self._closed = True
    +
    +    async def crash(self) -> None:
    +        if self._closed:
    +            return
    +        for task in self._tasks:
    +            task.cancel()
    +        if self._tasks:
    +            await asyncio.gather(*self._tasks, return_exceptions=True)
    +        for tp in self.accumulator.partitions():
    +            for pending in self.accumulator.pop(tp):
    +                if not pending.future.done():
    +                    pending.future.cancel()
    +        self._tasks.clear()
    +        self._closed = True
    +
    +    @property
    +    def owned_tasks(self) -> tuple[asyncio.Task[None], ...]:
    +        return tuple(task for task in self._tasks if not task.done())
    ```

**是什么，为什么现在需要**

生命周期所有权意味着组件负责启动、观测并关闭自己创建的每个资源；Failure Injector 让异步故障确定化；Lab 是可运行证据而非另一套实现。

**在运行时做什么**

Cluster 与 Producer 的 Close Path 停止新工作、Flush Pending Record、等待或取消自有 Task、暴露捕获失败，并允许重复 Close。

**关键语句理解**

后台异常必须穿过一个有所有权的 API Boundary；只记录日志却不改变可观察状态，会把数据丢失伪装成成功。

#### 包与工程支撑

保持包导出、依赖与测试环境可复现。

??? note "支撑文件差异（1 个文件）"
    **`src/minikafka/labs/__init__.py`**

    ```diff
    diff --git a/src/minikafka/labs/__init__.py b/src/minikafka/labs/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..3c531ad0bd75f1b3b088a44303829b006de1c002
    --- /dev/null
    +++ b/src/minikafka/labs/__init__.py
    @@ -0,0 +1 @@
    +"""Executable experiments that expose MiniKafka's semantic trade-offs."""
    ```


### 验证证据

运行 `uv run pytest -q $(cat journey/stages/18-lifecycle-labs/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

后台异常必须穿过一个有所有权的 API Boundary；只记录日志却不改变可观察状态，会把数据丢失伪装成成功。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 9 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/09-delivery-semantics.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/18-lifecycle-labs/stage.patch)
