from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.core.record import Record
from minikafka.replication.model import AckMode


@pytest.mark.asyncio
async def test_follower_pulls_complete_batches_from_its_leo(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        tp = TopicPartition("events", 0)
        replica_set = cluster.replica_set(tp)
        batch = RecordBatch.unassigned(
            (Record(None, b"a", 0), Record(None, b"b", 1))
        )
        await replica_set.append(batch, AckMode.LEADER)

        await replica_set.fetch_followers_once()

        assert replica_set.replicas[2].leo == 2
        assert [
            record.value
            for record in replica_set.replicas[2].log.fetch(0, 10)
        ] == [b"a", b"b"]


@pytest.mark.asyncio
async def test_cluster_can_pump_all_partition_followers(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 2, 2)
        producer = cluster.producer(batch_size=1)
        await producer.send("events", value=b"p0", partition=0)
        await producer.send("events", value=b"p1", partition=1)

        await cluster.replicate_all_once()

        assert cluster.replica_log(TopicPartition("events", 0), 2).leo == 1
        assert cluster.replica_log(TopicPartition("events", 1), 1).leo == 1
