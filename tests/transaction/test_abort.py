import asyncio
from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.replication.model import IsolationLevel


@pytest.mark.asyncio
async def test_aborted_records_remain_hidden(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("out", 1, 1)
        tx = await cluster.transactions.begin("tx-2")
        await tx.send("out", value=b"discard")
        await tx.abort()
        tp = TopicPartition("out", 0)

        assert cluster.fetch(
            tp,
            0,
            10,
            IsolationLevel.READ_COMMITTED,
        ) == ()
        assert await cluster.replica_set(tp).fetch(
            0,
            10,
            IsolationLevel.READ_COMMITTED,
        ) == ()


@pytest.mark.asyncio
async def test_abort_marker_waits_for_all_isr_replicas(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        broker_ids=(1, 2),
        min_insync_replicas=2,
    )
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("out", 1, 2)
        tx = await cluster.transactions.begin("tx-abort-acks-all")
        pending_send = asyncio.create_task(tx.send("out", value=b"aborted"))
        await asyncio.sleep(0)
        await cluster.replicate_all_once()
        await pending_send

        pending_abort = asyncio.create_task(tx.abort())
        await asyncio.sleep(0)
        assert not pending_abort.done()

        await cluster.replicate_all_once()
        await pending_abort
