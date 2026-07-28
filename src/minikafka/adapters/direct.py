from __future__ import annotations

from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicMetadata


class DirectAdmin:
    def __init__(self, cluster: BrokerCluster) -> None:
        self.cluster = cluster

    async def create_topic(
        self,
        name: str,
        partitions: int,
        replication_factor: int,
    ) -> TopicMetadata:
        return await self.cluster.create_topic(
            name,
            partitions,
            replication_factor,
        )

    async def describe_topic(self, name: str) -> TopicMetadata:
        return await self.cluster.describe_topic(name)
