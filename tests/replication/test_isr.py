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
async def test_lagging_follower_leaves_and_rejoins_isr(tmp_path: Path) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        broker_ids=(1, 2),
        replica_lag_max_offsets=0,
    )
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        replica_set = cluster.replica_set(TopicPartition("events", 0))

        await replica_set.append(
            RecordBatch.unassigned((Record(None, b"x", 0),)),
            AckMode.LEADER,
        )
        replica_set.refresh_isr()
        assert replica_set.isr == frozenset({1})

        await replica_set.fetch_followers_once()
        assert replica_set.isr == frozenset({1, 2})


@pytest.mark.asyncio
async def test_follower_expires_from_isr_by_fetch_time(tmp_path: Path) -> None:
    clock = ManualClock()
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        broker_ids=(1, 2),
        replica_lag_time_ms=100,
    )
    async with BrokerCluster.open(config, clock=clock) as cluster:
        await cluster.create_topic("events", 1, 2)
        replica_set = cluster.replica_set(TopicPartition("events", 0))
        clock.advance_ms(101)

        replica_set.refresh_isr()

        assert replica_set.isr == frozenset({1})
