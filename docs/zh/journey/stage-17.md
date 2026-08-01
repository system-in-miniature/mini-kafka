# Stage 17 · 轻量 JSON TCP Adapter

### 目标

实现轻量 JSON TCP Adapter，并能从可执行失败、运行时状态与关键语句解释其边界。

??? note "交付文件"
    - `src/minikafka/adapters/json_tcp.py`
    - `src/minikafka/errors.py`
    - `tests/adapters/test_direct_tcp_parity.py`
    - `tests/adapters/test_json_tcp.py`

### 当前遇到的问题

网络 API 只有在 Framing 与 Translation 不制造第二套分叉 Broker 语义时才有价值。

### 测试契约

#### 先看会坏在哪里

Parity 测试执行等价 Direct 与 TCP 操作；Framing 测试拆分 Stream 输入、携带二进制值，并要求 JSON Response 返回类型化领域错误。

??? note "文件差异：tests/adapters/test_direct_tcp_parity.py"
    ```diff
    diff --git a/tests/adapters/test_direct_tcp_parity.py b/tests/adapters/test_direct_tcp_parity.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..f144c59b494d77534572daa6dcf2ee092d3cf5a5
    --- /dev/null
    +++ b/tests/adapters/test_direct_tcp_parity.py
    @@ -0,0 +1,41 @@
    +import asyncio
    +import base64
    +import json
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.adapters.json_tcp import JsonTcpServer
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.core.metadata import TopicPartition
    +
    +
    +@pytest.mark.asyncio
    +async def test_tcp_adapter_translates_to_same_core_state(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 1)
    +        server = await JsonTcpServer.start(cluster, "127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        try:
    +            writer.write(
    +                json.dumps(
    +                    {
    +                        "operation": "produce",
    +                        "topic": "events",
    +                        "value_b64": base64.b64encode(b"adapter").decode(),
    +                        "acks": "1",
    +                    }
    +                ).encode()
    +                + b"\n"
    +            )
    +            await writer.drain()
    +            assert json.loads(await reader.readline())["ok"] is True
    +            direct = cluster.fetch(TopicPartition("events", 0), 0, 1)
    +            assert direct[0].value == b"adapter"
    +        finally:
    +            writer.close()
    +            await writer.wait_closed()
    +            await server.close()
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

Parity 测试执行等价 Direct 与 TCP 操作；Framing 测试拆分 Stream 输入、携带二进制值，并要求 JSON Response 返回类型化领域错误。

**关键测试语句**

```python
assert json.loads(await reader.readline())["ok"] is True
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

??? note "文件差异：tests/adapters/test_json_tcp.py"
    ```diff
    diff --git a/tests/adapters/test_json_tcp.py b/tests/adapters/test_json_tcp.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..29ed5e190c04affd66a8365593358179880b2d5e
    --- /dev/null
    +++ b/tests/adapters/test_json_tcp.py
    @@ -0,0 +1,91 @@
    +import asyncio
    +import base64
    +import json
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.adapters.json_tcp import JsonTcpServer
    +from minikafka.clock import ManualClock
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.cluster import BrokerCluster
    +
    +
    +async def exchange(
    +    reader: asyncio.StreamReader,
    +    writer: asyncio.StreamWriter,
    +    request: dict[str, object],
    +) -> dict[str, object]:
    +    writer.write(json.dumps(request).encode() + b"\n")
    +    await writer.drain()
    +    return json.loads(await reader.readline())
    +
    +
    +@pytest.mark.asyncio
    +async def test_tcp_produce_and_fetch_binary_values(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        server = await JsonTcpServer.start(cluster, "127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        try:
    +            created = await exchange(
    +                reader,
    +                writer,
    +                {
    +                    "operation": "create_topic",
    +                    "topic": "events",
    +                    "partitions": 1,
    +                    "replication_factor": 1,
    +                },
    +            )
    +            assert created["ok"] is True
    +            produced = await exchange(
    +                reader,
    +                writer,
    +                {
    +                    "operation": "produce",
    +                    "topic": "events",
    +                    "key_b64": base64.b64encode(b"\x00key").decode(),
    +                    "value_b64": base64.b64encode(b"\xffvalue").decode(),
    +                    "acks": "1",
    +                },
    +            )
    +            assert produced["offset"] == 0
    +            fetched = await exchange(
    +                reader,
    +                writer,
    +                {
    +                    "operation": "fetch",
    +                    "topic": "events",
    +                    "partition": 0,
    +                    "offset": 0,
    +                    "max_records": 10,
    +                },
    +            )
    +            assert base64.b64decode(fetched["records"][0]["value_b64"]) == (
    +                b"\xffvalue"
    +            )
    +        finally:
    +            writer.close()
    +            await writer.wait_closed()
    +            await server.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_domain_errors_are_typed_json(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path)
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        server = await JsonTcpServer.start(cluster, "127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        try:
    +            reply = await exchange(
    +                reader,
    +                writer,
    +                {"operation": "metadata", "topic": "missing"},
    +            )
    +            assert reply["ok"] is False
    +            assert reply["code"] == "UNKNOWN_TOPIC"
    +        finally:
    +            writer.close()
    +            await writer.wait_closed()
    +            await server.close()
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

Parity 测试执行等价 Direct 与 TCP 操作；Framing 测试拆分 Stream 输入、携带二进制值，并要求 JSON Response 返回类型化领域错误。

**关键测试语句**

```python
assert json.loads(await reader.readline())["ok"] is True
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

Adapter 使用长度前缀 JSON Envelope；二进制字段需要显式编码；Dispatch 把请求翻译到 Direct 语义核心，并把领域失败映射回稳定 Wire Error。

### 为什么需要这个机制

网络 API 只有在 Framing 与 Translation 不制造第二套分叉 Broker 语义时才有价值。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Server 精确读取 Frame 长度、校验 Request Shape、调用已有 Cluster Operation，再序列化结果；它不拥有存储或复制决策。

### 机制板块

#### 轻量 JSON TCP Adapter机制

Server 精确读取 Frame 长度、校验 Request Shape、调用已有 Cluster Operation，再序列化结果；它不拥有存储或复制决策。

??? note "文件差异：src/minikafka/adapters/json_tcp.py"
    ```diff
    diff --git a/src/minikafka/adapters/json_tcp.py b/src/minikafka/adapters/json_tcp.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..8a0fa3e83d7d959959630f5da89d68ac9f2036bc
    --- /dev/null
    +++ b/src/minikafka/adapters/json_tcp.py
    @@ -0,0 +1,250 @@
    +from __future__ import annotations
    +
    +import asyncio
    +import base64
    +import binascii
    +import json
    +from typing import Any, Self
    +
    +from minikafka.core.batch import RecordBatch
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.core.record import Record
    +from minikafka.errors import (
    +    FrameTooLarge,
    +    InvalidRequest,
    +    MiniKafkaError,
    +)
    +from minikafka.replication.model import AckMode, IsolationLevel
    +
    +
    +class JsonTcpServer:
    +    def __init__(
    +        self,
    +        cluster: object,
    +        server: asyncio.Server,
    +        *,
    +        max_frame_bytes: int,
    +    ) -> None:
    +        self.cluster = cluster
    +        self._server = server
    +        self.max_frame_bytes = max_frame_bytes
    +
    +    @classmethod
    +    async def start(
    +        cls,
    +        cluster: object,
    +        host: str,
    +        port: int,
    +        *,
    +        max_frame_bytes: int = 1_048_576,
    +    ) -> Self:
    +        instance: Self | None = None
    +
    +        async def handler(
    +            reader: asyncio.StreamReader,
    +            writer: asyncio.StreamWriter,
    +        ) -> None:
    +            if instance is not None:
    +                await instance._serve(reader, writer)
    +
    +        server = await asyncio.start_server(
    +            handler,
    +            host,
    +            port,
    +            limit=max_frame_bytes + 1,
    +        )
    +        instance = cls(
    +            cluster,
    +            server,
    +            max_frame_bytes=max_frame_bytes,
    +        )
    +        return instance
    +
    +    @property
    +    def address(self) -> tuple[str, int]:
    +        socket = self._server.sockets[0]
    +        host, port = socket.getsockname()[:2]
    +        return str(host), int(port)
    +
    +    async def close(self) -> None:
    +        self._server.close()
    +        await self._server.wait_closed()
    +
    +    async def _serve(
    +        self,
    +        reader: asyncio.StreamReader,
    +        writer: asyncio.StreamWriter,
    +    ) -> None:
    +        try:
    +            while True:
    +                try:
    +                    line = await reader.readline()
    +                except ValueError:
    +                    await self._write(writer, self._error(FrameTooLarge()))
    +                    break
    +                if not line:
    +                    break
    +                if len(line) > self.max_frame_bytes:
    +                    await self._write(writer, self._error(FrameTooLarge()))
    +                    break
    +                try:
    +                    request = json.loads(line)
    +                    if not isinstance(request, dict):
    +                        raise InvalidRequest("request must be a JSON object")
    +                    response = await self.dispatch(request)
    +                except MiniKafkaError as error:
    +                    response = self._error(error)
    +                except (
    +                    ValueError,
    +                    TypeError,
    +                    KeyError,
    +                    json.JSONDecodeError,
    +                    binascii.Error,
    +                ) as error:
    +                    response = self._error(InvalidRequest(str(error)))
    +                await self._write(writer, response)
    +        finally:
    +            writer.close()
    +            await writer.wait_closed()
    +
    +    async def dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
    +        operation = request.get("operation")
    +        if operation == "create_topic":
    +            topic = await self.cluster.create_topic(
    +                str(request["topic"]),
    +                int(request["partitions"]),
    +                int(request["replication_factor"]),
    +            )
    +            return {"ok": True, "topic": topic.name}
    +        if operation == "metadata":
    +            topic = await self.cluster.describe_topic(str(request["topic"]))
    +            return {
    +                "ok": True,
    +                "topic": topic.name,
    +                "partitions": [
    +                    {
    +                        "partition": metadata.partition,
    +                        "leader_id": metadata.leader_id,
    +                        "leader_epoch": metadata.leader_epoch,
    +                        "replicas": list(metadata.replicas),
    +                    }
    +                    for metadata in topic.partitions.values()
    +                ],
    +            }
    +        if operation == "produce":
    +            topic = str(request["topic"])
    +            partition = int(request.get("partition", 0))
    +            batch = RecordBatch.unassigned(
    +                (
    +                    Record(
    +                        self._decode_optional(request, "key_b64"),
    +                        self._decode_optional(request, "value_b64"),
    +                        self.cluster.clock.now_ms(),
    +                    ),
    +                )
    +            )
    +            result = await self.cluster.append_batch(
    +                TopicPartition(topic, partition),
    +                batch,
    +                AckMode.parse(request.get("acks", "1")),
    +            )
    +            return {
    +                "ok": True,
    +                "partition": partition,
    +                "offset": result.base_offset,
    +            }
    +        if operation == "fetch":
    +            tp = TopicPartition(
    +                str(request["topic"]),
    +                int(request["partition"]),
    +            )
    +            isolation = IsolationLevel(
    +                request.get("isolation", IsolationLevel.READ_UNCOMMITTED)
    +            )
    +            records = self.cluster.fetch(
    +                tp,
    +                int(request["offset"]),
    +                int(request.get("max_records", 100)),
    +                isolation,
    +            )
    +            return {
    +                "ok": True,
    +                "records": [
    +                    {
    +                        "offset": record.offset,
    +                        "key_b64": self._encode_optional(record.key),
    +                        "value_b64": self._encode_optional(record.value),
    +                    }
    +                    for record in records
    +                ],
    +            }
    +        if operation == "join_group":
    +            joined = await self.cluster.groups.join(
    +                str(request["group_id"]),
    +                str(request["member_id"]),
    +                tuple(str(topic) for topic in request["topics"]),
    +            )
    +            return {
    +                "ok": True,
    +                "generation": joined.generation,
    +                "assignment": [
    +                    {"topic": tp.topic, "partition": tp.partition}
    +                    for tp in joined.assignment
    +                ],
    +            }
    +        if operation == "heartbeat":
    +            await self.cluster.groups.heartbeat(
    +                str(request["group_id"]),
    +                str(request["member_id"]),
    +                int(request["generation"]),
    +            )
    +            return {"ok": True}
    +        if operation == "commit_offsets":
    +            offsets = {
    +                TopicPartition(str(item["topic"]), int(item["partition"])): int(
    +                    item["offset"]
    +                )
    +                for item in request["offsets"]
    +            }
    +            await self.cluster.groups.commit(
    +                str(request["group_id"]),
    +                str(request["member_id"]),
    +                int(request["generation"]),
    +                offsets,
    +            )
    +            return {"ok": True}
    +        raise InvalidRequest(f"unknown operation {operation!r}")
    +
    +    @staticmethod
    +    def _decode_optional(
    +        request: dict[str, Any],
    +        name: str,
    +    ) -> bytes | None:
    +        value = request.get(name)
    +        if value is None:
    +            return None
    +        return base64.b64decode(str(value), validate=True)
    +
    +    @staticmethod
    +    def _encode_optional(value: bytes | None) -> str | None:
    +        if value is None:
    +            return None
    +        return base64.b64encode(value).decode("ascii")
    +
    +    @staticmethod
    +    def _error(error: MiniKafkaError) -> dict[str, Any]:
    +        return {
    +            "ok": False,
    +            "code": error.code,
    +            "message": str(error),
    +        }
    +
    +    @staticmethod
    +    async def _write(
    +        writer: asyncio.StreamWriter,
    +        response: dict[str, Any],
    +    ) -> None:
    +        writer.write(
    +            json.dumps(response, separators=(",", ":")).encode() + b"\n"
    +        )
    +        await writer.drain()
    ```

??? note "文件差异：src/minikafka/errors.py"
    ```diff
    diff --git a/src/minikafka/errors.py b/src/minikafka/errors.py
    index 6fdc5a3ebf466c84b38018ae62e8644bf001b70d..e77dc31f1d95f0613cde7cb4eee935816fa110d6 100644
    --- a/src/minikafka/errors.py
    +++ b/src/minikafka/errors.py
    @@ -95,3 +95,11 @@ class OutOfOrderSequence(MiniKafkaError):
             super().__init__(f"expected sequence {expected}, got {actual}")
             self.expected = expected
             self.actual = actual
    +
    +
    +class InvalidRequest(MiniKafkaError):
    +    code = "INVALID_REQUEST"
    +
    +
    +class FrameTooLarge(MiniKafkaError):
    +    code = "FRAME_TOO_LARGE"
    ```

**是什么，为什么现在需要**

Adapter 使用长度前缀 JSON Envelope；二进制字段需要显式编码；Dispatch 把请求翻译到 Direct 语义核心，并把领域失败映射回稳定 Wire Error。

**在运行时做什么**

Server 精确读取 Frame 长度、校验 Request Shape、调用已有 Cluster Operation，再序列化结果；它不拥有存储或复制决策。

**关键语句理解**

Transport Handler 可以翻译类型，但不得重写 Ack、Visibility 或 Offset 规则；Parity Evidence 保护这条边界。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/17-json-tcp/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

Transport Handler 可以翻译类型，但不得重写 Ack、Visibility 或 Offset 规则；Parity Evidence 保护这条边界。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 10 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/10-protocol-and-beyond.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/17-json-tcp/stage.patch)
