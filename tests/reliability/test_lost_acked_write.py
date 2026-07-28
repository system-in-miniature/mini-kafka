from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition


@pytest.mark.asyncio
async def test_acks_one_can_confirm_a_leader_only_tail(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        producer = cluster.producer(batch_size=1, acks=1)

        acknowledged = await producer.send("events", value=b"at-risk")

        tp = TopicPartition("events", 0)
        assert acknowledged.offset == 0
        assert cluster.replica_log(tp, broker_id=1).leo == 1
        assert cluster.replica_log(tp, broker_id=2).leo == 0
        assert cluster.visible_end(tp) == 0

        await cluster.promote(tp, broker_id=2)

        assert cluster.leader_log(tp).leo == 0
        assert cluster.fetch(tp, 0, 10) == ()
