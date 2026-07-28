from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.core.record import Record
from minikafka.replication.model import AckMode


def sequenced() -> RecordBatch:
    return RecordBatch.unassigned(
        (Record(None, b"once", 0),),
        producer_id=9,
        producer_epoch=0,
        base_sequence=0,
    )


@pytest.mark.asyncio
async def test_duplicate_state_rebuilds_from_log(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1,))
    tp = TopicPartition("events", 0)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 1)
        first = await cluster.replica_set(tp).append(
            sequenced(),
            AckMode.LEADER,
        )

    async with BrokerCluster.open(config, clock=ManualClock()) as reopened:
        duplicate = await reopened.replica_set(tp).append(
            sequenced(),
            AckMode.LEADER,
        )
        assert duplicate.base_offset == first.base_offset
        assert reopened.leader_log(tp).leo == 1
