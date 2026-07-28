from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition


@pytest.mark.asyncio
async def test_committed_offset_survives_cluster_restart(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    tp = TopicPartition("events", 0)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 1)
        await cluster.producer(batch_size=1).send("events", value=b"one")
        consumer = cluster.consumer(group_id="g")
        consumer.assign((tp,))
        await consumer.poll(1)
        await consumer.commit()

    async with BrokerCluster.open(config, clock=ManualClock()) as reopened:
        consumer = reopened.consumer(group_id="g")
        consumer.assign((tp,))

        assert await consumer.committed(tp) == 1
        assert await consumer.poll(1) == ()
