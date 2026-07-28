from __future__ import annotations

from typing import Self

from minikafka.clock import Clock, SystemClock
from minikafka.config import MiniKafkaConfig
from minikafka.consumer.consumer import Consumer
from minikafka.consumer.group import GroupCoordinator
from minikafka.consumer.offsets import OffsetStore
from minikafka.core.batch import RecordBatch
from minikafka.core.metadata import (
    MetadataStore,
    PartitionMetadata,
    TopicMetadata,
    TopicPartition,
    round_robin_replicas,
    validate_topic_name,
)
from minikafka.core.record import StoredRecord
from minikafka.errors import (
    TopicAlreadyExists,
    UnknownPartition,
    UnknownTopic,
)
from minikafka.log.partition_log import PartitionLog
from minikafka.log.segment import LocatedBatch
from minikafka.producer.producer import Producer
from minikafka.replication.model import AckMode
from minikafka.replication.replica import Replica
from minikafka.replication.replica_set import PartitionReplicaSet


class BrokerCluster:
    def __init__(
        self,
        config: MiniKafkaConfig,
        metadata_store: MetadataStore,
        topics: dict[str, TopicMetadata],
        logs: dict[tuple[TopicPartition, int], PartitionLog],
        clock: Clock,
        offsets: OffsetStore,
    ) -> None:
        self.config = config
        self._metadata_store = metadata_store
        self._topics = topics
        self._logs = logs
        self.clock = clock
        self.offsets = offsets
        self.groups = GroupCoordinator(
            clock=clock,
            offset_store=offsets,
            topic_partitions=self._topic_partitions,
            session_timeout_ms=config.group_session_timeout_ms,
        )
        self._producers: set[Producer] = set()
        self._consumers: set[Consumer] = set()
        self._next_member_id = 1
        self._replica_sets: dict[TopicPartition, PartitionReplicaSet] = {}
        self._build_replica_sets()
        self._closed = False

    @classmethod
    def open(
        cls,
        config: MiniKafkaConfig,
        *,
        clock: Clock | None = None,
    ) -> BrokerCluster:
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
        return cls(
            config,
            metadata_store,
            topics,
            logs,
            SystemClock() if clock is None else clock,
            OffsetStore.open(config.data_dir / "offsets.json"),
        )

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
        for partition, metadata in partition_metadata.items():
            tp = TopicPartition(name, partition)
            self._replica_sets[tp] = self._make_replica_set(tp, metadata)
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

    async def append_leader_batch(
        self,
        tp: TopicPartition,
        batch: RecordBatch,
    ) -> LocatedBatch:
        self._ensure_open()
        result = await self.append_batch(tp, batch, AckMode.LEADER)
        return LocatedBatch(result.batch, 0, 0)

    async def append_batch(
        self,
        tp: TopicPartition,
        batch: RecordBatch,
        acks: AckMode | str | int,
    ):
        self._ensure_open()
        return await self.replica_set(tp).append(batch, acks)

    def producer(
        self,
        *,
        batch_size: int = 16_384,
        linger_ms: int = 0,
        max_buffer_bytes: int = 1_048_576,
        acks: AckMode | str | int = AckMode.LEADER,
    ) -> Producer:
        self._ensure_open()
        producer = Producer(
            self,
            self.clock,
            batch_size=batch_size,
            linger_ms=linger_ms,
            max_buffer_bytes=max_buffer_bytes,
            acks=acks,
        )
        self._producers.add(producer)
        return producer

    def debug_batch_count(self, tp: TopicPartition) -> int:
        return len(self.leader_log(tp).all_batches())

    def fetch(
        self,
        tp: TopicPartition,
        offset: int,
        max_records: int,
    ) -> tuple[StoredRecord, ...]:
        records = self.leader_log(tp).fetch(
            offset,
            max_records,
            end_offset=self.visible_end(tp),
        )
        return tuple(
            StoredRecord(
                topic=tp.topic,
                partition=tp.partition,
                offset=record.offset,
                key=record.key,
                value=record.value,
                timestamp_ms=record.timestamp_ms,
                headers=record.headers,
            )
            for record in records
        )

    def visible_end(self, tp: TopicPartition) -> int:
        return self.replica_set(tp).high_watermark

    def log_start_offset(self, tp: TopicPartition) -> int:
        return self.leader_log(tp).log_start_offset

    def consumer(
        self,
        *,
        group_id: str,
        auto_offset_reset: str = "earliest",
    ) -> Consumer:
        self._ensure_open()
        consumer = Consumer(
            self,
            group_id,
            auto_offset_reset=auto_offset_reset,
            member_id=f"member-{self._next_member_id}",
        )
        self._next_member_id += 1
        self._consumers.add(consumer)
        return consumer

    def _topic_partitions(self) -> dict[str, tuple[int, ...]]:
        return {
            topic.name: tuple(sorted(topic.partitions))
            for topic in self._topics.values()
        }

    async def expire_group_members(self) -> tuple[tuple[str, str], ...]:
        return await self.groups.expire_members()

    def replica_set(self, tp: TopicPartition) -> PartitionReplicaSet:
        self.partition_metadata(tp)
        return self._replica_sets[tp]

    def _make_replica_set(
        self,
        tp: TopicPartition,
        metadata: PartitionMetadata,
    ) -> PartitionReplicaSet:
        now = self.clock.now_ms()
        return PartitionReplicaSet(
            tp,
            metadata,
            {
                broker_id: Replica(
                    broker_id,
                    self._logs[(tp, broker_id)],
                    last_fetch_ms=now,
                )
                for broker_id in metadata.replicas
            },
            self.clock,
            self.config,
        )

    def _build_replica_sets(self) -> None:
        for topic in self._topics.values():
            for partition, metadata in topic.partitions.items():
                tp = TopicPartition(topic.name, partition)
                self._replica_sets[tp] = self._make_replica_set(tp, metadata)

    async def replicate_all_once(self) -> None:
        for tp in sorted(self._replica_sets):
            await self._replica_sets[tp].fetch_followers_once()

    async def close(self) -> None:
        if self._closed:
            return
        for producer in tuple(self._producers):
            await producer.close()
        for consumer in tuple(self._consumers):
            await consumer.close()
        self.offsets.flush()
        for log in self._logs.values():
            log.flush()
            log.close()
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("cluster is closed")
