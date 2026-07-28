from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.core.metadata import TopicPartition
from minikafka.replication.model import IsolationLevel
from minikafka.transaction.journal import TransactionJournal
from minikafka.transaction.model import TransactionData, TransactionState


@pytest.mark.asyncio
async def test_committed_transaction_visibility_survives_restart(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    tp = TopicPartition("out", 0)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("out", 1, 1)
        tx = await cluster.transactions.begin("durable")
        await tx.send("out", value=b"kept")
        await tx.commit()

    async with BrokerCluster.open(config, clock=ManualClock()) as reopened:
        assert [r.value for r in reopened.fetch(
            tp, 0, 10, IsolationLevel.READ_COMMITTED
        )] == [b"kept"]


@pytest.mark.asyncio
async def test_prepare_commit_finishes_offsets_during_recovery(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    input_tp = TopicPartition("input", 0)
    journal = TransactionJournal(tmp_path / "transactions.journal")
    journal.append(
        TransactionData(
            "recover-me",
            state=TransactionState.PREPARE_COMMIT,
            staged_offsets={"workers": {input_tp: 7}},
        )
    )

    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        assert await cluster.offsets.get("workers", input_tp) == 7


def test_transaction_journal_truncates_incomplete_tail(tmp_path: Path) -> None:
    path = tmp_path / "transactions.journal"
    journal = TransactionJournal(path)
    journal.append(TransactionData("valid"))
    valid_size = path.stat().st_size
    with path.open("ab") as file:
        file.write(b"deadbeef {")

    recovered = journal.recover()

    assert set(recovered) == {"valid"}
    assert path.stat().st_size == valid_size
