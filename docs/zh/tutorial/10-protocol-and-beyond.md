# 第 10 章 · 协议层与方法论

MiniKafka 核心可以直接调用，但真实分布式系统必须跨越 transport boundary。本章最后才加入这层边界，顺序是有意的：protocol adapter 应把请求翻译成已经定义的 domain operation，而不应暗中成为 topics、offsets、fencing 或 visibility 的第二套实现。

## 学习目标

学完本章，你将能够：

- 追踪一个 newline-delimited JSON 请求从 dispatch 到响应的过程；
- 解释 framing、二进制编码、类型化错误与 frame limit；
- 区分 Direct/TCP parity 与 Kafka client compatibility；
- 指出 MiniKafka 省略的真实 Kafka binary protocol 主要部件；以及
- 从本仓库继续进入有针对性的 Kafka 源码与协议阅读。

## Direct API 是语义基准

`src/minikafka/core/cluster.py` 的 `BrokerCluster` 持有 domain operation：topic 创建与 metadata、batch append、fetch、consumer group、replication、promotion 和 transaction。多数测试直接调用它，从而无需绑定 socket 就能看见调度与故障注入。

`src/minikafka/adapters/direct.py` 的 `DirectAdmin.create_topic` 与 `DirectAdmin.describe_topic` 几乎是透明 wrapper。producer 与 consumer 也由 `BrokerCluster` 创建。“Direct”不是玩具语义，而是没有 serialization 与 transport 的进程内 adapter。

transport adapter 是 `src/minikafka/adapters/json_tcp.py` 的 `JsonTcpServer`，它接收同一个 cluster 对象。关键架构箭头为：

```text
newline JSON -> validate/decode -> BrokerCluster operation
             <- encode result/error <- same domain state
```

不存在另一套 TCP log 或 consumer coordinator。因此，[架构指南](../architecture.md)建议最后阅读 `adapters/`。

## 连接与 framing

`JsonTcpServer.start` 调用 `asyncio.start_server` 并安装 per-connection handler。调用者可以传 port 0，让操作系统选择空闲端口；`JsonTcpServer.address` 读取实际绑定地址。`close` 停止接收并等待关闭。

`JsonTcpServer._serve` 把每行视作一个完整请求。这是一种简单 application framing protocol：

- `\n` 前的 bytes 是一个 UTF-8 JSON value；
- JSON value 必须是 object；
- 一个请求产生一个以换行结束的 JSON response；
- 一条连接可以顺序承载多个请求。

TCP 本身是 byte stream，不保留消息边界。没有 newline 规则，一次 `read` 可能得到半个 JSON 文档或多个文档。adapter 使用 `StreamReader.readline`，所以 framing 是显式的。

`max_frame_bytes` 以 stream limit 加一的形式传给 `asyncio.start_server`，读取后还会再次检查。超长 frame 返回类型化 `FRAME_TOO_LARGE` 错误并结束 serve loop。这限制了未认证行可消耗的内存，但只是教学防护，不是完整生产防御：没有 connection quota、authentication、TLS 或 per-principal rate limit。

在 `JsonTcpServer._serve` 中，domain `MiniKafkaError` 保留自己的 error code。JSON、类型、key、value 与 Base64 失败转换为 `InvalidRequest`。`JsonTcpServer._error` 把二者统一为：

```json
{"ok": false, "code": "SOME_CODE", "message": "..."}
```

未预料的编程或存储异常不会被宽泛转换，而是逃逸 request handler。这保留了 client error 与 terminal implementation failure 的区别。

## Dispatch 是翻译，不是业务逻辑

`JsonTcpServer.dispatch` 根据 `operation` 字段选择分支，支持 topic 创建、metadata、produce、fetch、group join、heartbeat 与 offset commit。每个分支解码 primitive JSON field，调用相应 cluster/coordinator 方法，再编码返回值。

produce 分支用 `src/minikafka/core/batch.py` 的 `RecordBatch.unassigned` 包装一条已解码记录，再以 `AckMode.parse` 的结果调用 `BrokerCluster.append_batch`。offset assignment、replication、ISR 检查与 idempotent state 仍位于 adapter 下层。

JSON string 无法安全承载任意 record bytes。`_decode_optional` 对 `key_b64` 与 `value_b64` 使用严格 Base64（`validate=True`），`_encode_optional` 执行反向转换。`null` 表示缺少 key/value，与空 bytes 的 Base64 表示 `""` 不同。

fetch 解析 `IsolationLevel`，调用 `BrokerCluster.fetch`，再 Base64 编码每条返回记录。group request 直接路由到 `GroupCoordinator.join`、`heartbeat` 与 `commit`，所以第 7 章的 generation 与 ownership 检查不变。未知 operation name 抛出 `InvalidRequest`。

adapter 刻意只暴露有限 operation set。TCP 上没有 transaction、manual promotion、follower fetch、retention 或 compaction operation。adapter 不暴露并不代表 core 不存在，而是教学 transport 的 public surface 更小。

## Parity 能证明什么，不能证明什么

`tests/adapters/test_direct_tcp_parity.py` 的 `test_tcp_adapter_translates_to_same_core_state` 通过 Direct API 创建 topic，经 TCP produce，再直接检查同一个 cluster state。`tests/adapters/test_json_tcp.py` 验证 binary-safe produce/fetch 和类型化 domain error。

这些测试通过能够证明：

- TCP request 到达同一 semantic core；
- 支持的 field 能无损通过 JSON/Base64；
- response framing 在真实 loopback stream 上工作；
- 选定 domain error 保留稳定 code。

它们**不能**证明 KafkaProducer、kafka-python、librdkafka 或 Kafka CLI 兼容；不能证明 protocol version negotiation、并发 request correlation 或生产网络行为。[行为矩阵](../behavior-matrix.md)把 adapter 称为“仅边界翻译”，并以 Direct API 作为核心 failure semantics 的权威路径。

## 与真实 Kafka wire protocol 对照

仓库的 [API、runtime 与 excluded systems 映射](../kafka-mapping.md)是规范差异指南。MiniKafka 是 Kafka data plane 的教学模型，**不是兼容 broker**。

Kafka 使用 length-prefixed binary request/response protocol。request header 携带 API key、API version、correlation ID 和 client ID；较新的 flexible version 使用 compact encoding 与 tagged field。每个 API 都有 versioned schema。client 会发现支持版本、metadata、leader 与 coordinator，再据此路由和重试。

MiniKafka 使用 newline JSON、operation string，没有 correlation ID、version field，并在每条连接上顺序响应。它的自定义 `MKB1` record-batch encoding 也既不是 Kafka on-disk record batch v2，也不是 Kafka wire representation。

Kafka 的真实接口包含 Produce、Fetch、Metadata、ApiVersions、FindCoordinator、JoinGroup、SyncGroup、Heartbeat、OffsetCommit、TxnOffsetCommit、InitProducerId、AddPartitionsToTxn、EndTxn 等众多 API。MiniKafka adapter 只暴露一个教学子集，并把若干协议合并为直接方法调用。

真实 Kafka 还包含 request encoding 之外的工作：

- broker-to-broker replication traffic 与 fetch session；
- controller/KRaft metadata propagation 与 leader election；
- authentication（SASL）、authorization、TLS、quota 与 throttling；
- listener 配置、advertised address、rack awareness 与 rolling compatibility；
- zero-copy/network buffer、backpressure、request queue 与 metrics；
- 跨混合 client/broker version 的协议演进。

编写 Kafka-compatible protocol server 会是另一个独立项目，而不是给 `JsonTcpServer` 加一个小功能。

## 动手实验：无需 socket 测试 dispatch

当前执行沙箱禁止绑定 loopback socket。我们仍可不启动 listener，直接构造 adapter 并调用 `dispatch`，从而测试 translation core。以下实验已在仓库实跑：

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run python - <<'PY'
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka import BrokerCluster, MiniKafkaConfig
from minikafka.adapters.json_tcp import JsonTcpServer
from minikafka.clock import ManualClock

async def main():
    with TemporaryDirectory() as d:
        async with BrokerCluster.open(
            MiniKafkaConfig(data_dir=Path(d)), clock=ManualClock()
        ) as cluster:
            server = object.__new__(JsonTcpServer)
            server.cluster = cluster
            server.max_frame_bytes = 1024
            print(await server.dispatch({
                "operation": "create_topic", "topic": "events",
                "partitions": 1, "replication_factor": 1,
            }))
            print(await server.dispatch({
                "operation": "produce", "topic": "events", "partition": 0,
                "value_b64": "aGVsbG8=", "acks": "1",
            }))
            print(await server.dispatch({
                "operation": "fetch", "topic": "events",
                "partition": 0, "offset": 0,
            }))

asyncio.run(main())
PY
```

实测输出：

```text
{'ok': True, 'topic': 'events'}
{'ok': True, 'partition': 0, 'offset': 0}
{'ok': True, 'records': [{'offset': 0, 'key_b64': None, 'value_b64': 'aGVsbG8='}]}
```

最后的 Base64 value 解码为 `hello`。这证明 dispatch 与共享 core state，但刻意不声称已经证明 TCP framing。

## 需运行时验证的实验：真实 TCP stream

**需运行时验证：**撰写本章的沙箱无法绑定 `127.0.0.1`。在普通本机运行：

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run pytest -q \
  tests/adapters/test_json_tcp.py tests/adapters/test_direct_tcp_parity.py
```

在支持 socket 的 runtime 上预期输出：

```text
...                                                                      [100%]
3 passed in <time>s
```

在撰写沙箱中，三个测试都到达 `JsonTcpServer.start`，并在 `asyncio.start_server` 失败：

```text
OSError: could not bind on any address out of [('127.0.0.1', 0)]
3 failed
```

这是环境限制，不是已实测 TCP 成功。声称 live transport 已验证之前，必须在非限制沙箱中重新运行命令。

## 下一步阅读真实 Kafka

把 MiniKafka 的 module boundary 当作地图，然后一次替换一个简化盒子：

1. 从 Apache Kafka protocol guide 与生成的 protocol message schema 开始，依次跟踪 `ApiVersions`、`Metadata`、`Produce`、`Fetch`。
2. 在 Kafka 源码中阅读 `SocketServer`、`KafkaApis` 与 request-channel handling 附近的网络请求路径。比较 dispatch ownership，而非 Python 语法。
3. 沿 `ReplicaManager`、partition state 和 follower fetch handling 跟踪分区复制，用真实后台调度重新理解 HW 与 ISR。
4. 把 group coordinator 和 assignor 实现与 JoinGroup、SyncGroup、heartbeat、offset API 对照阅读。
5. 结合 KIP-98 阅读 transaction coordinator 与 transaction state manager，把 producer ID/epoch 连接到 marker 与 `read_committed`。
6. 专门学习 KIP-101/KIP-279 leader-epoch reconciliation，因为 MiniKafka 基于 HW 的 promotion 刻意不可迁移。

仓库的 [Kafka 映射](../kafka-mapping.md)为每个机制列出了相关配置与 KIP，是从本书章节进入真实实现的最佳桥梁。

## 练习

1. **理解题：** 为什么这里需要 Base64？`None` 与 `b""` 如何表示为不同值？

    ??? note "参考答案"

        JSON string 是 Unicode，而 Kafka key/value 是任意 bytes。Base64 提供可逆 ASCII 表示。`None` 是 JSON `null`；空 bytes 编码成空 Base64 string `""`，因此 tombstone 与 empty value 保持不同。

2. **动手题：** 把 dispatch 实验复制到 `/tmp/protocol_lab.py`，通过 `dispatch` 发送非法 Base64。因为 `_serve` 通常负责映射 decoding exception，这里请自行捕获并打印 exception type。验收：打印 `Error`（来自严格 `binascii`），且没有记录追加。不要修改 `src/`。

    ??? note "参考答案"

        在 produce operation 中使用 `"***"`。`_decode_optional(..., validate=True)` 在 `BrokerCluster.append_batch` 前抛出错误。直接 `dispatch` 没有安装 `_serve` 的异常映射；真实 stream 上会得到 `INVALID_REQUEST` 对象。

3. **动手协议设计：** 在 scratch Markdown 文件中规定一个 length-prefixed request header，包含 API key、version、correlation ID 和 payload length。验收：给出 encoder/decoder 伪代码，拒绝负数或超大 length，并在每个 response 回显 correlation ID。

    ??? note "参考答案"

        合格设计使用固定 network-byte-order header，在分配内存前校验声明的 frame length，按 `(api_key, version)` dispatch，并把 correlation ID 复制进 response header。它还必须定义 unknown API、unsupported version、partial read 与 multiple in-flight request 的行为。这说明 newline JSON parity 为什么不等于 Kafka compatibility。

4. **理解题：** 在说“Kafka client 可以连接 MiniKafka”之前，需要什么证据？

    ??? note "参考答案"

        server 必须实现 Kafka binary framing 与 versioned schema，然后至少用真实 client 对 ApiVersions、metadata discovery、produce、fetch 做 live interoperability test。当前 Direct/TCP parity test 使用 MiniKafka 自己的 JSON protocol，不能支持该声明。

## 小结

`JsonTcpServer` 是薄 adapter：newline framing 与 Base64 把有限 JSON operation set 翻译成同一 `BrokerCluster` 状态转换。这种分离正是本章重点。它证明 transport/core 架构，却不假装自定义 JSON service 实现了 Kafka 的 binary、versioned、correlated、security-aware protocol。接下来应当沿映射逐机制进入真实 Kafka，并保留本书贯穿的方法：隔离一个不变量，阅读持有它的代码，运行聚焦故障的实验，明确写出简化边界。
