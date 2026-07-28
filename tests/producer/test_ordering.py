from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition


@pytest.mark.asyncio
async def test_same_key_keeps_partition_local_order(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 3, 1)
        producer = cluster.producer(batch_size=4096, linger_ms=1000)
        pending = [
            producer.send("events", key=b"user-42", value=str(index).encode())
            for index in range(5)
        ]

        await producer.flush()
        metadata = [await item for item in pending]

        assert len({item.partition for item in metadata}) == 1
        tp = TopicPartition("events", metadata[0].partition)
        assert [record.value for record in cluster.leader_log(tp).fetch(0, 10)] == [
            b"0",
            b"1",
            b"2",
            b"3",
            b"4",
        ]


@pytest.mark.asyncio
async def test_explicit_partition_overrides_keyed_partition(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 3, 1)
        producer = cluster.producer(batch_size=1, linger_ms=1000)

        metadata = await producer.send(
            "events",
            key=b"would-hash-elsewhere",
            value=b"x",
            partition=2,
        )

        assert metadata.partition == 2


@pytest.mark.asyncio
async def test_keyless_partition_rotates_after_batch_closes(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 3, 1)
        producer = cluster.producer(batch_size=1, linger_ms=1000)

        first = await producer.send("events", value=b"a")
        second = await producer.send("events", value=b"b")

        assert second.partition == (first.partition + 1) % 3
