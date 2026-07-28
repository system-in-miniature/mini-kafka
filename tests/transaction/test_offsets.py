from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition


@pytest.mark.asyncio
async def test_output_and_input_offset_publish_together(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    input_tp = TopicPartition("input", 0)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("input", 1, 1)
        await cluster.create_topic("output", 1, 1)
        tx = await cluster.transactions.begin("processor")
        await tx.send("output", value=b"result")
        await tx.send_offsets("workers", {input_tp: 4})
        assert await cluster.offsets.get("workers", input_tp) is None

        await tx.commit()

        assert await cluster.offsets.get("workers", input_tp) == 4


@pytest.mark.asyncio
async def test_abort_discards_staged_offsets(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    input_tp = TopicPartition("input", 0)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("input", 1, 1)
        tx = await cluster.transactions.begin("processor")
        await tx.send_offsets("workers", {input_tp: 4})
        await tx.abort()
        assert await cluster.offsets.get("workers", input_tp) is None
