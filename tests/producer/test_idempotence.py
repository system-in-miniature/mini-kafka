import asyncio
from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.core.record import Record
from minikafka.errors import OutOfOrderSequence, ProducerFenced
from minikafka.replication.model import AckMode


def sequenced(sequence: int, value: bytes, *, epoch: int = 0) -> RecordBatch:
    return RecordBatch.unassigned(
        (Record(None, value, 0),),
        producer_id=7,
        producer_epoch=epoch,
        base_sequence=sequence,
    )


@pytest.mark.asyncio
async def test_exact_retry_returns_original_offsets(tmp_path: Path) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        broker_ids=(1, 2),
        min_insync_replicas=2,
    )
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        replica_set = cluster.replica_set(TopicPartition("events", 0))
        batch = sequenced(0, b"x")
        pending = asyncio.create_task(replica_set.append(batch, AckMode.ALL))
        await asyncio.sleep(0)
        await replica_set.fetch_followers_once()
        first = await pending

        second = await replica_set.append(batch, AckMode.ALL)

        assert second == first
        assert replica_set.leader.leo == 1


@pytest.mark.asyncio
async def test_sequence_gap_and_old_epoch_are_rejected(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1,))
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 1)
        replica_set = cluster.replica_set(TopicPartition("events", 0))
        await replica_set.append(sequenced(0, b"first"), AckMode.LEADER)
        with pytest.raises(OutOfOrderSequence):
            await replica_set.append(sequenced(2, b"gap"), AckMode.LEADER)
        await replica_set.append(
            sequenced(0, b"new epoch", epoch=1),
            AckMode.LEADER,
        )
        with pytest.raises(ProducerFenced):
            await replica_set.append(
                sequenced(1, b"old epoch", epoch=0),
                AckMode.LEADER,
            )


@pytest.mark.asyncio
async def test_named_producer_fences_previous_instance(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1,))
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 1)
        old = cluster.producer(
            transactional_name="writer",
            idempotent=True,
            batch_size=1,
        )
        cluster.producer(
            transactional_name="writer",
            idempotent=True,
            batch_size=1,
        )

        with pytest.raises(ProducerFenced):
            await old.send("events", value=b"stale")
