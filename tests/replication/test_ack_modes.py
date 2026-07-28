import asyncio
from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.core.record import Record
from minikafka.errors import NotEnoughReplicas, NotEnoughReplicasAfterAppend
from minikafka.replication.model import AckMode


def batch(value: bytes) -> RecordBatch:
    return RecordBatch.unassigned((Record(None, value, 0),))


@pytest.mark.asyncio
async def test_acks_all_waits_for_acknowledgement_set(tmp_path: Path) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        broker_ids=(1, 2),
        min_insync_replicas=2,
    )
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        replica_set = cluster.replica_set(TopicPartition("events", 0))

        pending = asyncio.create_task(replica_set.append(batch(b"safe"), AckMode.ALL))
        await asyncio.sleep(0)
        assert not pending.done()

        await replica_set.fetch_followers_once()

        assert (await pending).next_offset == 1


@pytest.mark.asyncio
async def test_acks_all_rejects_insufficient_isr_before_append(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        broker_ids=(1, 2),
        min_insync_replicas=2,
    )
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        replica_set = cluster.replica_set(TopicPartition("events", 0))
        replica_set.remove_from_isr(2)

        with pytest.raises(NotEnoughReplicas):
            await replica_set.append(batch(b"rejected"), AckMode.ALL)

        assert replica_set.leader.leo == 0


@pytest.mark.asyncio
async def test_acks_all_fails_if_isr_shrinks_after_append(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        broker_ids=(1, 2),
        min_insync_replicas=2,
    )
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        replica_set = cluster.replica_set(TopicPartition("events", 0))
        pending = asyncio.create_task(replica_set.append(batch(b"x"), AckMode.ALL))
        await asyncio.sleep(0)

        replica_set.remove_from_isr(2)

        with pytest.raises(NotEnoughReplicasAfterAppend):
            await pending
        assert replica_set.leader.leo == 1


@pytest.mark.asyncio
async def test_acks_zero_returns_unknown_offsets(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 1)
        producer = cluster.producer(batch_size=1, acks=0)

        metadata = await producer.send("events", value=b"fire-and-forget")

        assert metadata.offset is None
        assert metadata.base_offset is None
        assert cluster.leader_log(TopicPartition("events", 0)).leo == 1
