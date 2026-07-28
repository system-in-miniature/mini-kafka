import asyncio
from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition


async def produce_values(cluster: BrokerCluster, values: list[bytes]) -> None:
    producer = cluster.producer(batch_size=4096, linger_ms=1000)
    pending = [producer.send("events", value=value) for value in values]
    await producer.flush()
    await asyncio.gather(*pending)


@pytest.mark.asyncio
async def test_position_and_commit_are_independent(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 1)
        await produce_values(cluster, [b"0", b"1", b"2"])
        tp = TopicPartition("events", 0)
        consumer = cluster.consumer(group_id="g", auto_offset_reset="earliest")
        consumer.assign((tp,))

        records = await consumer.poll(max_records=2)

        assert [record.offset for record in records] == [0, 1]
        assert consumer.position(tp) == 2
        assert await consumer.committed(tp) is None
        assert await consumer.lag(tp) == 1

        await consumer.commit()
        assert await consumer.committed(tp) == 2
        consumer.seek(tp, 0)
        assert (await consumer.poll(1))[0].offset == 0


@pytest.mark.asyncio
async def test_latest_consumer_starts_at_log_end(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 1)
        await produce_values(cluster, [b"old"])
        tp = TopicPartition("events", 0)
        consumer = cluster.consumer(group_id="latest", auto_offset_reset="latest")
        consumer.assign((tp,))

        assert await consumer.poll(1) == ()
        assert consumer.position(tp) == 1


@pytest.mark.asyncio
async def test_poll_distributes_record_budget_across_assignment(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 2, 1)
        producer = cluster.producer(batch_size=1)
        await producer.send("events", value=b"p0", partition=0)
        await producer.send("events", value=b"p1", partition=1)
        consumer = cluster.consumer(group_id="g")
        consumer.assign(
            (TopicPartition("events", 0), TopicPartition("events", 1))
        )

        records = await consumer.poll(max_records=1)

        assert len(records) == 1
