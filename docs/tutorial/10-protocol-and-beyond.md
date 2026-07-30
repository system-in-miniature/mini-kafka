# Chapter 10 · Protocol Layer and Beyond

MiniKafka's core can be called directly, but real distributed systems cross a
transport boundary. This chapter adds that boundary last. The ordering is
intentional: a protocol adapter should translate requests into already-defined
domain operations; it should not secretly become a second implementation of
topics, offsets, fencing, or visibility.

## Learning objectives

By the end of this chapter, you can:

- trace one newline-delimited JSON request through dispatch and back;
- explain framing, binary encoding, typed errors, and frame limits;
- distinguish Direct/TCP parity from Kafka client compatibility;
- identify major parts of Kafka's real binary protocol that MiniKafka omits;
  and
- continue from this repository into targeted Kafka source and protocol study.

## The Direct API is the semantic reference

`src/minikafka/core/cluster.py`, `BrokerCluster`, owns the domain operations:
topic creation and metadata, batch append, fetch, consumer groups, replication,
promotion, and transactions. Most tests call it directly. This keeps scheduling
and failure injection visible without binding correctness to sockets.

`src/minikafka/adapters/direct.py`, `DirectAdmin.create_topic` and
`DirectAdmin.describe_topic`, are almost transparent wrappers. Producers and
consumers are also created from `BrokerCluster`. “Direct” does not mean toy
semantics; it means an in-process adapter with no serialization or transport.

The transport adapter is `src/minikafka/adapters/json_tcp.py`,
`JsonTcpServer`. It accepts the same cluster object. The important architectural
arrow is:

```text
newline JSON -> validate/decode -> BrokerCluster operation
             <- encode result/error <- same domain state
```

There is no separate TCP log or consumer coordinator. The
[architecture guide](../architecture.md) therefore recommends reading
`adapters/` last.

## Connection and framing

`JsonTcpServer.start` calls `asyncio.start_server` and installs a per-connection
handler. The caller may pass port zero to let the operating system choose a
free port; `JsonTcpServer.address` reads the bound socket address. `close`
stops accepting and waits for closure.

`JsonTcpServer._serve` treats each line as one complete request. This is a
simple application framing protocol:

- bytes before `\n` are one UTF-8 JSON value;
- the JSON value must be an object;
- one request produces one newline-terminated JSON response; and
- the connection may carry multiple sequential requests.

TCP itself is a byte stream and preserves no message boundaries. Without the
newline rule, one `read` could contain half a JSON document or several. The
adapter uses `StreamReader.readline`, so framing is explicit.

`max_frame_bytes` is passed to `asyncio.start_server` as a stream limit plus one
and checked again after read. An overlong frame returns a typed
`FRAME_TOO_LARGE` error and closes that serve loop. This bounds memory consumed
by an unauthenticated line. It is a teaching safeguard, not a complete
production defense: there are no connection quotas, authentication, TLS, or
per-principal rate limits.

In `JsonTcpServer._serve`, domain `MiniKafkaError` exceptions retain their error
code. JSON, type, key, value, and Base64 failures become `InvalidRequest`.
`JsonTcpServer._error` normalizes both into:

```json
{"ok": false, "code": "SOME_CODE", "message": "..."}
```

Unexpected programming or storage exceptions are not broadly converted; they
escape the request handler. That preserves the distinction between a client
error and a terminal implementation failure.

## Dispatch is translation, not business logic

`JsonTcpServer.dispatch` selects the `operation` field. It supports topic
creation, metadata, produce, fetch, group join, heartbeat, and offset commit.
Each branch decodes primitive JSON fields, calls the corresponding cluster or
coordinator method, and encodes the returned value.

For produce, the adapter uses
`src/minikafka/core/batch.py`, `RecordBatch.unassigned`, around one decoded
record, then calls `BrokerCluster.append_batch` with `AckMode.parse`. Offset
assignment, replication, ISR checks, and idempotent state remain below the
adapter.

JSON strings cannot safely carry arbitrary record bytes. `_decode_optional`
uses strict Base64 (`validate=True`) for `key_b64` and `value_b64`;
`_encode_optional` reverses it. `null` represents a missing key or value,
remaining distinct from the Base64 encoding of empty bytes (`""`).

Fetch parses `IsolationLevel`, calls `BrokerCluster.fetch`, and Base64-encodes
each returned record. Group requests route directly to
`GroupCoordinator.join`, `heartbeat`, and `commit`, so the generation and
ownership checks described in Chapter 7 are unchanged. Unknown operation names
raise `InvalidRequest`.

The adapter intentionally exposes only a bounded operation set. There are no
TCP operations for transactions, manual promotion, follower fetch, retention,
or compaction. Absence from the adapter does not mean absence from the core;
it means the teaching transport has a smaller public surface.

## What parity does and does not prove

`tests/adapters/test_direct_tcp_parity.py`,
`test_tcp_adapter_translates_to_same_core_state`, creates a topic through the
Direct API, produces through TCP, and then checks the same cluster state
directly. `tests/adapters/test_json_tcp.py` verifies binary-safe produce/fetch
and typed domain errors.

Passing those tests proves:

- a TCP request reaches the same semantic core;
- supported fields survive JSON/Base64 translation;
- response framing works on a real loopback stream; and
- selected domain errors retain stable codes.

It does **not** prove compatibility with KafkaProducer, kafka-python, librdkafka,
or Kafka's command-line tools. It does not prove protocol version negotiation,
request correlation under concurrency, or production networking behavior. The
[behavior matrix](../behavior-matrix.md) calls the adapter “boundary
translation only” and keeps the Direct API authoritative for core failure
semantics.

## Compared with the real Kafka wire protocol

The repository's
[API, runtime, and excluded-systems mapping](../kafka-mapping.md#api-runtime-and-excluded-systems)
is the canonical difference guide. MiniKafka is a teaching model of Kafka's
data plane, **not a compatible broker**.

Kafka uses a length-prefixed binary request/response protocol. Request headers
carry an API key, API version, correlation ID, and client ID; newer flexible
versions use compact encodings and tagged fields. Each API has versioned
schemas. Clients discover supported versions, metadata, leaders, and
coordinators, then route and retry requests accordingly.

MiniKafka instead uses newline JSON, operation strings, no correlation ID, no
version field, and sequential responses on each connection. Its custom `MKB1`
record-batch encoding is also neither Kafka's on-disk record batch v2 nor its
wire representation.

Kafka's real surface includes Produce, Fetch, Metadata, ApiVersions,
FindCoordinator, JoinGroup, SyncGroup, Heartbeat, OffsetCommit,
TxnOffsetCommit, InitProducerId, AddPartitionsToTxn, EndTxn, and many more.
MiniKafka's adapter exposes a small pedagogical subset and collapses several
protocols into direct method calls.

Real Kafka also includes concerns outside request encoding:

- broker-to-broker replication traffic and fetch sessions;
- controller/KRaft metadata propagation and leader elections;
- authentication (SASL), authorization, TLS, quotas, and throttling;
- listener configuration, advertised addresses, rack awareness, and rolling
  compatibility;
- zero-copy/network buffering, backpressure, request queues, and metrics; and
- protocol evolution across mixed client and broker versions.

Writing a Kafka-compatible protocol server would be a separate project, not a
small extension to `JsonTcpServer`.

## Hands-on experiment: test dispatch without sockets

The current execution sandbox forbids binding a loopback socket. We can still
test the translation core by constructing the adapter without starting its
listener and calling `dispatch`. This experiment was run in the repository:

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

Measured output:

```text
{'ok': True, 'topic': 'events'}
{'ok': True, 'partition': 0, 'offset': 0}
{'ok': True, 'records': [{'offset': 0, 'key_b64': None, 'value_b64': 'aGVsbG8='}]}
```

The final Base64 value decodes to `hello`. This proves dispatch and shared core
state, but deliberately makes no claim about TCP framing.

## Runtime-required experiment: real TCP stream

**Requires runtime verification:** the sandbox used to author this chapter
cannot bind `127.0.0.1`. On a normal local machine, run:

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run pytest -q \
  tests/adapters/test_json_tcp.py tests/adapters/test_direct_tcp_parity.py
```

Expected output on a socket-capable runtime:

```text
...                                                                      [100%]
3 passed in <time>s
```

In the authoring sandbox, all three tests reached `JsonTcpServer.start` and
failed at `asyncio.start_server` with:

```text
OSError: could not bind on any address out of [('127.0.0.1', 0)]
3 failed
```

That is an environment restriction, not measured TCP success. Re-running the
command outside the restricted sandbox is required before claiming live
transport verification.

## Where to read real Kafka next

Use MiniKafka's module boundaries as a map, then replace one simplified box at a
time:

1. Start with the Apache Kafka protocol guide and generated protocol message
   schemas. Follow `ApiVersions`, `Metadata`, `Produce`, and `Fetch`.
2. In Kafka source, read the network request path around `SocketServer`,
   `KafkaApis`, and request-channel handling. Compare dispatch ownership, not
   Python syntax.
3. Follow partition replication through `ReplicaManager`, partition state, and
   follower fetch handling. Revisit HW and ISR with real background scheduling.
4. Read the group coordinator and assignor implementations alongside JoinGroup,
   SyncGroup, heartbeat, and offset APIs.
5. Read the transaction coordinator and transaction state manager with KIP-98;
   connect producer IDs and epochs to markers and `read_committed`.
6. Study KIP-101/KIP-279 leader-epoch reconciliation specifically, because
   MiniKafka's HW-based promotion is intentionally not transferable.

The repository's [Kafka mapping](../kafka-mapping.md) names relevant
configurations and KIPs for each mechanism, making it the best bridge from a
chapter to real implementation study.

## Exercises

1. **Understanding:** Why is Base64 necessary here, and how are `None` and
   `b""` represented differently?

    ??? note "Reference answer"

        JSON strings are Unicode, while Kafka keys and values are arbitrary
        bytes. Base64 gives a reversible ASCII representation. `None` is JSON
        `null`; empty bytes encode as the empty Base64 string `""`, so tombstone
        and empty value remain distinct.

2. **Hands-on:** Copy the dispatch experiment to `/tmp/protocol_lab.py` and
   send an invalid Base64 value through `dispatch`. Since `_serve` normally
   maps decoding exceptions, wrap the call and print the exception type.
   Acceptance: it prints `Error` (from strict `binascii`) and no record is
   appended. Do not edit `src/`.

    ??? note "Reference answer"

        Use a value such as `"***"` with the produce operation.
        `_decode_optional(..., validate=True)` raises before
        `BrokerCluster.append_batch`. Direct `dispatch` does not install the
        `_serve` exception mapping; on a real stream the response would be an
        `INVALID_REQUEST` object.

3. **Hands-on protocol design:** In a scratch Markdown file, specify a
   length-prefixed request header containing API key, version, correlation ID,
   and payload length. Acceptance: provide encoder/decoder pseudocode that
   rejects negative or oversized lengths and echoes correlation ID in every
   response.

    ??? note "Reference answer"

        A sufficient design uses a fixed network-byte-order header, validates
        the declared frame length before allocation, dispatches by `(api_key,
        version)`, and copies correlation ID into the response header. It must
        also define behavior for unknown APIs, unsupported versions, partial
        reads, and multiple in-flight requests. This illustrates why newline
        JSON parity is not Kafka compatibility.

4. **Understanding:** Which evidence would be needed before saying “a Kafka
   client can connect to MiniKafka”?

    ??? note "Reference answer"

        The server would need Kafka binary framing and versioned schemas, then
        live interoperability tests with a real client covering ApiVersions,
        metadata discovery, produce, and fetch at minimum. The current
        Direct/TCP parity tests use MiniKafka's own JSON protocol and cannot
        support that claim.

## Summary

`JsonTcpServer` is a thin adapter: newline framing and Base64 translate a
bounded JSON operation set into the same `BrokerCluster` state transitions.
That separation is the point of the chapter. It proves transport/core
architecture without pretending that a custom JSON service implements Kafka's
binary, versioned, correlated, security-aware protocol. Continue into real
Kafka by following the mapping mechanism by mechanism—and preserve the method
used throughout this book: isolate an invariant, read its owning code, run a
failure-focused experiment, and state the simplification boundary explicitly.
