from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.replication.model import IsolationLevel


@pytest.mark.asyncio
async def test_read_committed_waits_for_commit_marker(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("out", 1, 1)
        tx = await cluster.transactions.begin("tx-1")
        await tx.send("out", value=b"pending")
        tp = TopicPartition("out", 0)

        assert [r.value for r in cluster.fetch(
            tp, 0, 10, IsolationLevel.READ_UNCOMMITTED
        )] == [b"pending"]
        assert cluster.fetch(tp, 0, 10, IsolationLevel.READ_COMMITTED) == ()

        await tx.commit()

        assert [r.value for r in cluster.fetch(
            tp, 0, 10, IsolationLevel.READ_COMMITTED
        )] == [b"pending"]
        assert all(batch.control is not None for batch in
                   cluster.leader_log(tp).all_batches()[1:])
