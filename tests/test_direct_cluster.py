from pathlib import Path

import pytest

from minikafka.adapters.direct import DirectAdmin
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.errors import TopicAlreadyExists, UnknownTopic


@pytest.mark.asyncio
async def test_create_topic_builds_partition_replica_logs(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))

    async with BrokerCluster.open(config) as cluster:
        topic = await cluster.create_topic(
            "orders",
            partitions=3,
            replication_factor=2,
        )

        assert tuple(topic.partitions) == (0, 1, 2)
        assert topic.partitions[0].replicas == (1, 2)
        assert cluster.replica_log(TopicPartition("orders", 0), 1).leo == 0
        assert cluster.replica_log(TopicPartition("orders", 0), 2).leo == 0
        with pytest.raises(TopicAlreadyExists):
            await cluster.create_topic("orders", 1, 1)


@pytest.mark.asyncio
async def test_direct_admin_delegates_to_cluster(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(7,))

    async with BrokerCluster.open(config) as cluster:
        admin = DirectAdmin(cluster)
        created = await admin.create_topic("events", 1, 1)

        assert await admin.describe_topic("events") == created
        with pytest.raises(UnknownTopic):
            await admin.describe_topic("missing")


@pytest.mark.asyncio
async def test_metadata_and_logs_survive_restart(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    async with BrokerCluster.open(config) as cluster:
        await cluster.create_topic("events", 2, 2)

    async with BrokerCluster.open(config) as reopened:
        topic = await reopened.describe_topic("events")
        assert topic.partitions[1].leader_id == 2
        assert reopened.replica_log(TopicPartition("events", 1), 1).leo == 0
