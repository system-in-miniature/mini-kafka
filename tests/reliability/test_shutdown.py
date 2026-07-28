import asyncio
from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.lifecycle import LifecycleState


@pytest.mark.asyncio
async def test_graceful_close_drains_producer_and_is_idempotent(
    tmp_path: Path,
) -> None:
    cluster = BrokerCluster.open(
        MiniKafkaConfig(data_dir=tmp_path),
        clock=ManualClock(),
    )
    await cluster.create_topic("events", 1, 1)
    producer = cluster.producer(linger_ms=1_000)
    pending = producer.send("events", value=b"flush-me")

    await cluster.close()
    await cluster.close()

    assert (await pending).offset == 0
    assert cluster.state is LifecycleState.CLOSED
    assert cluster.owned_tasks == ()

    reopened = BrokerCluster.open(
        MiniKafkaConfig(data_dir=tmp_path),
        clock=ManualClock(),
    )
    assert reopened.leader_log(TopicPartition("events", 0)).leo == 1
    await reopened.close()


@pytest.mark.asyncio
async def test_crash_does_not_drain_buffered_producer(
    tmp_path: Path,
) -> None:
    cluster = BrokerCluster.open(
        MiniKafkaConfig(data_dir=tmp_path),
        clock=ManualClock(),
    )
    await cluster.create_topic("events", 1, 1)
    producer = cluster.producer(linger_ms=1_000)
    pending = producer.send("events", value=b"lost")

    await cluster.crash()

    assert pending.cancelled()
    assert cluster.state is LifecycleState.CLOSED
    await asyncio.sleep(0)
