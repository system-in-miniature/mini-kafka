import asyncio
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
async def test_read_committed_waits_for_commit_marker(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("out", 1, 1)
        tx = await cluster.transactions.begin("tx-1")
        await tx.send("out", value=b"pending")
        tp = TopicPartition("out", 0)

        assert [r.value for r in cluster.fetch(
            tp, 0, 10, IsolationLevel.READ_UNCOMMITTED
        )] == [b"pending"]
        assert cluster.fetch(tp, 0, 10, IsolationLevel.READ_COMMITTED) == ()

        await tx.commit()

        assert [r.value for r in cluster.fetch(
            tp, 0, 10, IsolationLevel.READ_COMMITTED
        )] == [b"pending"]
        assert all(batch.control is not None for batch in
                   cluster.leader_log(tp).all_batches()[1:])


@pytest.mark.asyncio
async def test_transaction_data_waits_for_all_isr_replicas(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        broker_ids=(1, 2),
        min_insync_replicas=2,
    )
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("out", 1, 2)
        tx = await cluster.transactions.begin("tx-data-acks-all")
        pending = asyncio.create_task(tx.send("out", value=b"replicated"))
        await asyncio.sleep(0)

        assert not pending.done()

        await cluster.replicate_all_once()
        await pending


@pytest.mark.asyncio
async def test_commit_marker_waits_for_all_isr_replicas(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        broker_ids=(1, 2),
        min_insync_replicas=2,
    )
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("out", 1, 2)
        tx = await cluster.transactions.begin("tx-commit-acks-all")
        pending_send = asyncio.create_task(tx.send("out", value=b"committed"))
        await asyncio.sleep(0)
        await cluster.replicate_all_once()
        await pending_send

        pending_commit = asyncio.create_task(tx.commit())
        await asyncio.sleep(0)
        assert not pending_commit.done()

        await cluster.replicate_all_once()
        await pending_commit


@pytest.mark.asyncio
async def test_replica_set_read_committed_uses_last_stable_offset(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("out", 1, 1)
        tp = TopicPartition("out", 0)
        replica_set = cluster.replica_set(tp)
        tx = await cluster.transactions.begin("tx-direct-fetch")
        await tx.send("out", value=b"pending")
        await replica_set.append(
            RecordBatch.unassigned((Record(None, b"later", 0),)),
            AckMode.LEADER,
        )

        assert await replica_set.fetch(
            0,
            10,
            IsolationLevel.READ_COMMITTED,
        ) == ()

        await tx.commit()

        assert [
            record.value
            for record in await replica_set.fetch(
                0,
                10,
                IsolationLevel.READ_COMMITTED,
            )
        ] == [b"pending", b"later"]
