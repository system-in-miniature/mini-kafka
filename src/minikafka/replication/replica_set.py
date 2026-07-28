from __future__ import annotations

import asyncio

from minikafka.clock import Clock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.metadata import PartitionMetadata, TopicPartition
from minikafka.core.record import LogRecord
from minikafka.replication.model import AckMode, IsolationLevel, ProduceResult
from minikafka.replication.replica import Replica


class PartitionReplicaSet:
    def __init__(
        self,
        topic_partition: TopicPartition,
        metadata: PartitionMetadata,
        replicas: dict[int, Replica],
        clock: Clock,
        config: MiniKafkaConfig,
    ) -> None:
        self.topic_partition = topic_partition
        self.leader_id = metadata.leader_id
        self.leader_epoch = metadata.leader_epoch
        self.replicas = replicas
        self.clock = clock
        self.config = config
        leader_leo = self.leader.leo
        self._isr = {
            broker_id
            for broker_id, replica in replicas.items()
            if replica.leo == leader_leo
        }
        self._isr.add(self.leader_id)
        for broker_id, replica in replicas.items():
            replica.in_sync = broker_id in self._isr
        self.high_watermark = min(replica.leo for replica in replicas.values())
        self._lock = asyncio.Lock()

    @property
    def leader(self) -> Replica:
        return self.replicas[self.leader_id]

    @property
    def followers(self) -> tuple[Replica, ...]:
        return tuple(
            replica
            for broker_id, replica in sorted(self.replicas.items())
            if broker_id != self.leader_id
        )

    @property
    def follower_ids(self) -> tuple[int, ...]:
        return tuple(replica.broker_id for replica in self.followers)

    @property
    def isr(self) -> frozenset[int]:
        return frozenset(self._isr)

    async def append(
        self,
        batch: RecordBatch,
        acks: AckMode | str | int = AckMode.LEADER,
    ) -> ProduceResult:
        mode = AckMode.parse(acks)
        async with self._lock:
            located = self.leader.log.append(batch)
            self._advance_high_watermark()
            return ProduceResult(
                batch=located.batch,
                base_offset=(
                    located.batch.base_offset
                    if mode is not AckMode.NONE
                    else None
                ),
                next_offset=located.batch.next_offset,
                offsets_known=mode is not AckMode.NONE,
            )

    async def fetch_followers_once(self) -> None:
        async with self._lock:
            for follower in self.followers:
                batches = self.leader.log.read_batches(
                    follower.leo,
                    self.config.replica_fetch_max_bytes,
                )
                for batch in batches:
                    follower.log.append_replica_batch(batch)
                follower.last_fetch_ms = self.clock.now_ms()
            self.refresh_isr()

    def refresh_isr(self) -> None:
        now = self.clock.now_ms()
        leader_leo = self.leader.leo
        next_isr = {self.leader_id}
        for follower in self.followers:
            within_offset = (
                leader_leo - follower.leo
                <= self.config.replica_lag_max_offsets
            )
            within_time = (
                now - follower.last_fetch_ms
                <= self.config.replica_lag_time_ms
            )
            if within_offset and within_time:
                next_isr.add(follower.broker_id)
        self._isr = next_isr
        for broker_id, replica in self.replicas.items():
            replica.in_sync = broker_id in next_isr
        self._advance_high_watermark()

    def remove_from_isr(self, broker_id: int) -> None:
        if broker_id == self.leader_id:
            raise ValueError("leader cannot be removed from ISR")
        self._isr.discard(broker_id)
        self.replicas[broker_id].in_sync = False
        self._advance_high_watermark()

    def _advance_high_watermark(self) -> None:
        candidate = min(
            self.replicas[broker_id].leo for broker_id in self._isr
        )
        self.high_watermark = max(self.high_watermark, candidate)

    async def fetch(
        self,
        offset: int,
        max_records: int,
        isolation: IsolationLevel = IsolationLevel.READ_UNCOMMITTED,
    ) -> tuple[LogRecord, ...]:
        del isolation
        return self.leader.log.fetch(
            offset,
            max_records,
            end_offset=self.high_watermark,
        )
