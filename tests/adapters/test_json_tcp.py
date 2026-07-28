import asyncio
import base64
import json
from pathlib import Path

import pytest

from minikafka.adapters.json_tcp import JsonTcpServer
from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster


async def exchange(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    request: dict[str, object],
) -> dict[str, object]:
    writer.write(json.dumps(request).encode() + b"\n")
    await writer.drain()
    return json.loads(await reader.readline())


@pytest.mark.asyncio
async def test_tcp_produce_and_fetch_binary_values(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        server = await JsonTcpServer.start(cluster, "127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(*server.address)
        try:
            created = await exchange(
                reader,
                writer,
                {
                    "operation": "create_topic",
                    "topic": "events",
                    "partitions": 1,
                    "replication_factor": 1,
                },
            )
            assert created["ok"] is True
            produced = await exchange(
                reader,
                writer,
                {
                    "operation": "produce",
                    "topic": "events",
                    "key_b64": base64.b64encode(b"\x00key").decode(),
                    "value_b64": base64.b64encode(b"\xffvalue").decode(),
                    "acks": "1",
                },
            )
            assert produced["offset"] == 0
            fetched = await exchange(
                reader,
                writer,
                {
                    "operation": "fetch",
                    "topic": "events",
                    "partition": 0,
                    "offset": 0,
                    "max_records": 10,
                },
            )
            assert base64.b64decode(fetched["records"][0]["value_b64"]) == (
                b"\xffvalue"
            )
        finally:
            writer.close()
            await writer.wait_closed()
            await server.close()


@pytest.mark.asyncio
async def test_domain_errors_are_typed_json(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        server = await JsonTcpServer.start(cluster, "127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(*server.address)
        try:
            reply = await exchange(
                reader,
                writer,
                {"operation": "metadata", "topic": "missing"},
            )
            assert reply["ok"] is False
            assert reply["code"] == "UNKNOWN_TOPIC"
        finally:
            writer.close()
            await writer.wait_closed()
            await server.close()
