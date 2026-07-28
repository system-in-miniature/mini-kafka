import asyncio
from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.core.record import Record
from minikafka.replication.model import AckMode


def batch(value: bytes) -> RecordBatch:
    return RecordBatch.unassigned((Record(None, value, 0),))


@pytest.mark.asyncio
async def test_old_leader_truncates_uncommitted_tail(tmp_path: Path) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        broker_ids=(1, 2),
        min_insync_replicas=2,
    )
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        tp = TopicPartition("events", 0)
        replica_set = cluster.replica_set(tp)
        committed = asyncio.create_task(
            replica_set.append(batch(b"committed"), AckMode.ALL)
        )
        await asyncio.sleep(0)
        await replica_set.fetch_followers_once()
        await committed
        await replica_set.append(batch(b"leader-only"), AckMode.LEADER)

        await cluster.promote(tp, broker_id=2)

        assert replica_set.high_watermark == 1
        assert replica_set.replicas[1].leo == 2
        await replica_set.rejoin(1)
        assert replica_set.replicas[1].leo == 1
        await replica_set.fetch_followers_once()
        assert replica_set.replicas[1].leo == replica_set.leader.leo
