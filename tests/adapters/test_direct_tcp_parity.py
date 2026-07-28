import asyncio
import base64
import json
from pathlib import Path

import pytest

from minikafka.adapters.json_tcp import JsonTcpServer
from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition


@pytest.mark.asyncio
async def test_tcp_adapter_translates_to_same_core_state(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 1)
        server = await JsonTcpServer.start(cluster, "127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(*server.address)
        try:
            writer.write(
                json.dumps(
                    {
                        "operation": "produce",
                        "topic": "events",
                        "value_b64": base64.b64encode(b"adapter").decode(),
                        "acks": "1",
                    }
                ).encode()
                + b"\n"
            )
            await writer.drain()
            assert json.loads(await reader.readline())["ok"] is True
            direct = cluster.fetch(TopicPartition("events", 0), 0, 1)
            assert direct[0].value == b"adapter"
        finally:
            writer.close()
            await writer.wait_closed()
            await server.close()
