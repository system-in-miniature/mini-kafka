from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.core.record import Record
from minikafka.replication.model import AckMode, IsolationLevel


@pytest.mark.asyncio
async def test_high_watermark_is_minimum_isr_leo(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        replica_set = cluster.replica_set(TopicPartition("events", 0))

        result = await replica_set.append(
            RecordBatch.unassigned((Record(None, b"x", 0),)),
            AckMode.LEADER,
        )

        assert result.next_offset == 1
        assert replica_set.leader.leo == 1
        assert replica_set.high_watermark == 0
        await replica_set.fetch_followers_once()
        assert replica_set.high_watermark == 1


@pytest.mark.asyncio
async def test_consumer_cannot_read_above_high_watermark(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        tp = TopicPartition("events", 0)
        replica_set = cluster.replica_set(tp)
        await replica_set.append(
            RecordBatch.unassigned((Record(None, b"hidden", 0),)),
            AckMode.LEADER,
        )

        assert await replica_set.fetch(
            0,
            10,
            IsolationLevel.READ_COMMITTED,
        ) == ()
        consumer = cluster.consumer(group_id="g")
        consumer.assign((tp,))
        assert await consumer.poll(1) == ()

        await replica_set.fetch_followers_once()
        assert (await consumer.poll(1))[0].value == b"hidden"


@pytest.mark.asyncio
async def test_single_replica_append_immediately_advances_hw(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 1)
        replica_set = cluster.replica_set(TopicPartition("events", 0))

        await replica_set.append(
            RecordBatch.unassigned((Record(None, b"visible", 0),)),
            AckMode.LEADER,
        )

        assert replica_set.high_watermark == 1
