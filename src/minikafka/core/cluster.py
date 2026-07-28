from __future__ import annotations

from typing import Self

from minikafka.config import MiniKafkaConfig
from minikafka.core.metadata import (
    MetadataStore,
    PartitionMetadata,
    TopicMetadata,
    TopicPartition,
    round_robin_replicas,
    validate_topic_name,
)
from minikafka.errors import (
    TopicAlreadyExists,
    UnknownPartition,
    UnknownTopic,
)
from minikafka.log.partition_log import PartitionLog


class BrokerCluster:
    def __init__(
        self,
        config: MiniKafkaConfig,
        metadata_store: MetadataStore,
        topics: dict[str, TopicMetadata],
        logs: dict[tuple[TopicPartition, int], PartitionLog],
    ) -> None:
        self.config = config
        self._metadata_store = metadata_store
        self._topics = topics
        self._logs = logs
        self._closed = False

    @classmethod
    def open(cls, config: MiniKafkaConfig) -> BrokerCluster:
        metadata_store = MetadataStore(config.data_dir / "metadata.json")
        topics = metadata_store.load()
        logs: dict[tuple[TopicPartition, int], PartitionLog] = {}
        try:
            for topic in topics.values():
                for partition in topic.partitions.values():
                    tp = TopicPartition(topic.name, partition.partition)
                    for broker_id in partition.replicas:
                        logs[(tp, broker_id)] = PartitionLog.open(
                            config.data_dir
                            / f"broker-{broker_id}"
                            / f"{topic.name}-{partition.partition}",
                            config,
                            leader_epoch=partition.leader_epoch,
                        )
        except BaseException:
            for log in logs.values():
                log.close()
            raise
        return cls(config, metadata_store, topics, logs)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def create_topic(
        self,
        name: str,
        partitions: int,
        replication_factor: int,
    ) -> TopicMetadata:
        self._ensure_open()
        validate_topic_name(name)
        if name in self._topics:
            raise TopicAlreadyExists(name)
        assignment = round_robin_replicas(
            self.config.broker_ids,
            partitions,
            replication_factor,
        )
        partition_metadata = {
            partition: PartitionMetadata(
                topic=name,
                partition=partition,
                replicas=replicas,
                leader_id=replicas[0],
            )
            for partition, replicas in assignment.items()
        }
        topic = TopicMetadata(name, partition_metadata)
        opened: dict[tuple[TopicPartition, int], PartitionLog] = {}
        try:
            for partition, metadata in partition_metadata.items():
                tp = TopicPartition(name, partition)
                for broker_id in metadata.replicas:
                    opened[(tp, broker_id)] = PartitionLog.open(
                        self.config.data_dir
                        / f"broker-{broker_id}"
                        / f"{name}-{partition}",
                        self.config,
                    )
            prospective = dict(self._topics)
            prospective[name] = topic
            self._metadata_store.save(prospective)
        except BaseException:
            for log in opened.values():
                log.close()
            raise
        self._logs.update(opened)
        self._topics[name] = topic
        return topic

    async def describe_topic(self, name: str) -> TopicMetadata:
        self._ensure_open()
        try:
            return self._topics[name]
        except KeyError as error:
            raise UnknownTopic(name) from error

    def topic(self, name: str) -> TopicMetadata:
        self._ensure_open()
        try:
            return self._topics[name]
        except KeyError as error:
            raise UnknownTopic(name) from error

    def partition_metadata(self, tp: TopicPartition) -> PartitionMetadata:
        topic = self.topic(tp.topic)
        try:
            return topic.partitions[tp.partition]
        except KeyError as error:
            raise UnknownPartition(f"{tp.topic}-{tp.partition}") from error

    def replica_log(self, tp: TopicPartition, broker_id: int) -> PartitionLog:
        self.partition_metadata(tp)
        try:
            return self._logs[(tp, broker_id)]
        except KeyError as error:
            raise UnknownPartition(
                f"broker {broker_id} has no replica for {tp}"
            ) from error

    def leader_log(self, tp: TopicPartition) -> PartitionLog:
        metadata = self.partition_metadata(tp)
        return self.replica_log(tp, metadata.leader_id)

    async def close(self) -> None:
        if self._closed:
            return
        for log in self._logs.values():
            log.flush()
            log.close()
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("cluster is closed")
