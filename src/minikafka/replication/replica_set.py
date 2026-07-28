from __future__ import annotations

import asyncio
from dataclasses import dataclass

from minikafka.clock import Clock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.metadata import PartitionMetadata, TopicPartition
from minikafka.core.record import LogRecord
from minikafka.errors import (
    FencedLeaderEpoch,
    NotEnoughReplicas,
    NotEnoughReplicasAfterAppend,
    NotInSyncReplica,
)
from minikafka.replication.model import AckMode, IsolationLevel, ProduceResult
from minikafka.replication.replica import Replica


@dataclass(slots=True)
class AckWaiter:
    target_offset: int
    acknowledgement_set: frozenset[int]
    result: ProduceResult
    future: asyncio.Future[ProduceResult]


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
        self._ack_waiters: list[AckWaiter] = []

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
        *,
        leader_epoch: int | None = None,
    ) -> ProduceResult:
        mode = AckMode.parse(acks)
        waiter: AckWaiter | None = None
        async with self._lock:
            if leader_epoch is not None and leader_epoch != self.leader_epoch:
                raise FencedLeaderEpoch(
                    f"leader epoch {leader_epoch} does not match "
                    f"{self.leader_epoch}"
                )
            if (
                mode is AckMode.ALL
                and len(self._isr) < self.config.min_insync_replicas
            ):
                raise NotEnoughReplicas(
                    f"ISR size {len(self._isr)} is below "
                    f"{self.config.min_insync_replicas}"
                )
            located = self.leader.log.append(batch)
            self._advance_high_watermark()
            result = ProduceResult(
                batch=located.batch,
                base_offset=(
                    located.batch.base_offset
                    if mode is not AckMode.NONE
                    else None
                ),
                next_offset=located.batch.next_offset,
                offsets_known=mode is not AckMode.NONE,
            )
            if mode is AckMode.ALL:
                waiter = AckWaiter(
                    target_offset=located.batch.next_offset,
                    acknowledgement_set=frozenset(self._isr),
                    result=result,
                    future=asyncio.get_running_loop().create_future(),
                )
                self._ack_waiters.append(waiter)
                self._resolve_ack_waiters()
            else:
                return result
        if waiter is None:
            raise RuntimeError("acks=all did not create a waiter")
        return await waiter.future

    async def fetch_followers_once(self) -> None:
        async with self._lock:
            for follower in self.followers:
                if follower.leo > self.leader.leo:
                    follower.log.truncate_to(self.high_watermark)
                batches = self.leader.log.read_batches(
                    follower.leo,
                    self.config.replica_fetch_max_bytes,
                )
                for batch in batches:
                    follower.log.append_replica_batch(batch)
                follower.last_fetch_ms = self.clock.now_ms()
            self.refresh_isr()

    async def promote(self, broker_id: int) -> None:
        async with self._lock:
            if broker_id not in self._isr:
                raise NotInSyncReplica(
                    f"broker {broker_id} is not in the ISR"
                )
            safe_end = self.high_watermark
            promoted = self.replicas[broker_id]
            if promoted.leo > safe_end:
                promoted.log.truncate_to(safe_end)
            self.leader_id = broker_id
            self.leader_epoch += 1
            for replica in self.replicas.values():
                replica.log.leader_epoch = self.leader_epoch
                replica.in_sync = replica.broker_id == broker_id
            self._isr = {broker_id}
            self.high_watermark = safe_end
            for waiter in self._ack_waiters:
                if not waiter.future.done():
                    waiter.future.set_exception(
                        FencedLeaderEpoch("leader changed before acknowledgement")
                    )
            self._ack_waiters.clear()

    async def rejoin(self, broker_id: int) -> None:
        async with self._lock:
            if broker_id == self.leader_id:
                raise ValueError("leader cannot rejoin itself as a follower")
            replica = self.replicas[broker_id]
            if replica.leo > self.high_watermark:
                replica.log.truncate_to(self.high_watermark)
            replica.log.leader_epoch = self.leader_epoch
            replica.last_fetch_ms = self.clock.now_ms()
            replica.in_sync = False
            self._isr.discard(broker_id)

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
        self._resolve_ack_waiters()

    def _resolve_ack_waiters(self) -> None:
        for waiter in self._ack_waiters:
            if waiter.future.done():
                continue
            if len(self._isr) < self.config.min_insync_replicas:
                waiter.future.set_exception(
                    NotEnoughReplicasAfterAppend(
                        "ISR fell below min.insync.replicas after append"
                    )
                )
                continue
            if all(
                self.replicas[broker_id].leo >= waiter.target_offset
                for broker_id in waiter.acknowledgement_set
            ):
                waiter.future.set_result(waiter.result)
        self._ack_waiters = [
            waiter for waiter in self._ack_waiters if not waiter.future.done()
        ]

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
