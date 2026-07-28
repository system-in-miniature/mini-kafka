from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.core.record import Record
from minikafka.errors import FencedLeaderEpoch, NotInSyncReplica
from minikafka.replication.model import AckMode


def batch(value: bytes) -> RecordBatch:
    return RecordBatch.unassigned((Record(None, value, 0),))


@pytest.mark.asyncio
async def test_only_isr_replica_can_be_promoted(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        tp = TopicPartition("events", 0)
        replica_set = cluster.replica_set(tp)
        replica_set.remove_from_isr(2)

        with pytest.raises(NotInSyncReplica):
            await cluster.promote(tp, broker_id=2)


@pytest.mark.asyncio
async def test_replica_behind_high_watermark_cannot_reenter_isr(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        broker_ids=(1, 2),
        replica_lag_max_offsets=10,
    )
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        tp = TopicPartition("events", 0)
        replica_set = cluster.replica_set(tp)
        await replica_set.append(batch(b"committed"), AckMode.LEADER)
        await replica_set.fetch_followers_once()
        assert replica_set.high_watermark == 1
        replica_set.remove_from_isr(2)
        replica_set.replicas[2].log.truncate_to(0)

        replica_set.refresh_isr()

        assert 2 not in replica_set.isr
        with pytest.raises(NotInSyncReplica):
            await cluster.promote(tp, broker_id=2)


@pytest.mark.asyncio
async def test_promotion_increments_epoch_and_fences_old_requests(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        tp = TopicPartition("events", 0)

        await cluster.promote(tp, broker_id=2)

        replica_set = cluster.replica_set(tp)
        assert replica_set.leader_id == 2
        assert replica_set.leader_epoch == 1
        with pytest.raises(FencedLeaderEpoch):
            await replica_set.append(
                batch(b"stale"),
                AckMode.LEADER,
                leader_epoch=0,
            )


@pytest.mark.asyncio
async def test_promoted_metadata_survives_restart(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    tp = TopicPartition("events", 0)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("events", 1, 2)
        await cluster.promote(tp, broker_id=2)

    async with BrokerCluster.open(config, clock=ManualClock()) as reopened:
        assert reopened.partition_metadata(tp).leader_id == 2
        assert reopened.partition_metadata(tp).leader_epoch == 1
        assert reopened.replica_set(tp).leader_id == 2
