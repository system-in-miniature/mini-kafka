import asyncio
from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.errors import ProducerBufferFull


@pytest.mark.asyncio
async def test_linger_flushes_one_partition_batch(tmp_path: Path) -> None:
    clock = ManualClock(100)
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=clock) as cluster:
        await cluster.create_topic("events", 1, 1)
        producer = cluster.producer(batch_size=4096, linger_ms=10)

        first = producer.send("events", key=b"k", value=b"1")
        second = producer.send("events", key=b"k", value=b"2")
        assert not first.done()
        clock.advance_ms(10)
        await producer.run_due_flushes()

        assert (await first).base_offset == 0
        assert (await second).offset == 1
        assert cluster.debug_batch_count(TopicPartition("events", 0)) == 1


@pytest.mark.asyncio
async def test_batch_size_flushes_without_waiting_for_linger(tmp_path: Path) -> None:
    clock = ManualClock(0)
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=clock) as cluster:
        await cluster.create_topic("events", 1, 1)
        producer = cluster.producer(batch_size=68, linger_ms=1000)

        first = producer.send("events", value=b"a")
        second = producer.send("events", value=b"b")

        assert (await first).offset == 0
        assert (await second).offset == 1
        assert cluster.debug_batch_count(TopicPartition("events", 0)) == 1
        await asyncio.wait_for(producer.close(), timeout=0.1)


@pytest.mark.asyncio
async def test_producer_buffer_is_bounded(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 1)
        producer = cluster.producer(
            batch_size=4096,
            linger_ms=1000,
            max_buffer_bytes=40,
        )

        producer.send("events", value=b"a")
        with pytest.raises(ProducerBufferFull):
            producer.send("events", value=b"b" * 20)

        await producer.flush()
