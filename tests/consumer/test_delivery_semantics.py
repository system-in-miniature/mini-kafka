from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition


@pytest.mark.asyncio
async def test_process_before_commit_can_replay_after_crash(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    tp = TopicPartition("events", 0)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 1)
        await cluster.producer(batch_size=1).send("events", value=b"work")
        first = cluster.consumer(group_id="workers")
        first.assign((tp,))
        assert (await first.poll(1))[0].value == b"work"
        # Processing happened, but the next offset was not committed.

        restarted = cluster.consumer(group_id="workers")
        restarted.assign((tp,))
        replayed = await restarted.poll(1)

        assert replayed[0].offset == 0


@pytest.mark.asyncio
async def test_commit_before_process_can_skip_after_crash(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    tp = TopicPartition("events", 0)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 1)
        await cluster.producer(batch_size=1).send("events", value=b"work")
        first = cluster.consumer(group_id="workers")
        first.assign((tp,))
        await first.poll(1)
        await first.commit()
        # Crash occurs before application processing.

        restarted = cluster.consumer(group_id="workers")
        restarted.assign((tp,))

        assert await restarted.poll(1) == ()
