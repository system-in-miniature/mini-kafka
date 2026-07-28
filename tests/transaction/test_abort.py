from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.replication.model import IsolationLevel


@pytest.mark.asyncio
async def test_aborted_records_remain_hidden(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("out", 1, 1)
        tx = await cluster.transactions.begin("tx-2")
        await tx.send("out", value=b"discard")
        await tx.abort()

        assert cluster.fetch(
            TopicPartition("out", 0),
            0,
            10,
            IsolationLevel.READ_COMMITTED,
        ) == ()
