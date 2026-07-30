from __future__ import annotations

import zlib

from minikafka.core.batch import ControlType, RecordBatch
from minikafka.core.metadata import TopicPartition
from minikafka.core.record import Record
from minikafka.replication.model import AckMode
from minikafka.transaction.journal import TransactionJournal
from minikafka.transaction.model import TransactionData, TransactionState


class Transaction:
    def __init__(
        self,
        manager: TransactionManager,
        data: TransactionData,
    ) -> None:
        self.manager = manager
        self.data = data

    async def send(
        self,
        topic: str,
        *,
        value: bytes | None,
        key: bytes | None = None,
        partition: int | None = None,
    ) -> None:
        await self.manager.send(
            self.data,
            topic,
            value=value,
            key=key,
            partition=partition,
        )

    async def send_offsets(
        self,
        group_id: str,
        offsets: dict[TopicPartition, int],
    ) -> None:
        self.data.staged_offsets.setdefault(group_id, {}).update(offsets)
        self.manager.journal.append(self.data)

    async def commit(self) -> None:
        await self.manager.commit(self.data)

    async def abort(self) -> None:
        await self.manager.abort(self.data)


class TransactionManager:
    def __init__(self, cluster: object, journal: TransactionJournal) -> None:
        self.cluster = cluster
        self.journal = journal
        self._transactions = journal.recover()
        self._finish_recovery()

    async def begin(self, transaction_id: str) -> Transaction:
        existing = self._transactions.get(transaction_id)
        if existing is not None and existing.state in {
            TransactionState.ONGOING,
            TransactionState.PREPARE_COMMIT,
            TransactionState.PREPARE_ABORT,
        }:
            raise RuntimeError(f"transaction {transaction_id} is active")
        data = TransactionData(transaction_id)
        self._transactions[transaction_id] = data
        self.journal.append(data)
        return Transaction(self, data)

    async def send(
        self,
        data: TransactionData,
        topic: str,
        *,
        value: bytes | None,
        key: bytes | None,
        partition: int | None,
    ) -> None:
        self._require_ongoing(data)
        metadata = self.cluster.topic(topic)
        selected = (
            partition
            if partition is not None
            else (
                0
                if key is None
                else (zlib.crc32(key) & 0xFFFFFFFF) % len(metadata.partitions)
            )
        )
        tp = TopicPartition(topic, selected)
        batch = RecordBatch.unassigned(
            (Record(key, value, self.cluster.clock.now_ms()),),
            transactional_id=data.transaction_id,
        )
        result = await self.cluster.append_batch(tp, batch, AckMode.ALL)
        data.partitions.add(tp)
        if result.base_offset is not None:
            data.first_offsets.setdefault(tp, result.base_offset)
        self.journal.append(data)

    async def commit(self, data: TransactionData) -> None:
        self._require_ongoing(data)
        data.state = TransactionState.PREPARE_COMMIT
        self.journal.append(data)
        for tp in sorted(data.partitions):
            await self.cluster.append_batch(
                tp,
                RecordBatch.control_marker(
                    transaction_id=data.transaction_id,
                    control=ControlType.COMMIT,
                ),
                AckMode.ALL,
            )
        await self.cluster.offsets.commit_many(data.staged_offsets)
        data.state = TransactionState.COMPLETE_COMMIT
        self.journal.append(data)

    async def abort(self, data: TransactionData) -> None:
        self._require_ongoing(data)
        data.state = TransactionState.PREPARE_ABORT
        self.journal.append(data)
        for tp in sorted(data.partitions):
            await self.cluster.append_batch(
                tp,
                RecordBatch.control_marker(
                    transaction_id=data.transaction_id,
                    control=ControlType.ABORT,
                ),
                AckMode.ALL,
            )
        data.staged_offsets.clear()
        data.state = TransactionState.COMPLETE_ABORT
        self.journal.append(data)

    def is_committed(self, transaction_id: str) -> bool:
        transaction = self._transactions.get(transaction_id)
        return (
            transaction is not None
            and transaction.state is TransactionState.COMPLETE_COMMIT
        )

    def last_stable_offset(self, tp: TopicPartition, high_watermark: int) -> int:
        unstable = [
            transaction.first_offsets[tp]
            for transaction in self._transactions.values()
            if tp in transaction.first_offsets
            and transaction.state
            in {
                TransactionState.ONGOING,
                TransactionState.PREPARE_COMMIT,
                TransactionState.PREPARE_ABORT,
            }
        ]
        return min(unstable, default=high_watermark)

    @staticmethod
    def _require_ongoing(data: TransactionData) -> None:
        if data.state is not TransactionState.ONGOING:
            raise RuntimeError(
                f"transaction {data.transaction_id} is {data.state.value}"
            )

    def _finish_recovery(self) -> None:
        for transaction in self._transactions.values():
            if transaction.state is TransactionState.PREPARE_COMMIT:
                self.cluster.offsets.commit_many_sync(
                    transaction.staged_offsets
                )
                transaction.state = TransactionState.COMPLETE_COMMIT
                self.journal.append(transaction)
            elif transaction.state is TransactionState.PREPARE_ABORT:
                transaction.staged_offsets.clear()
                transaction.state = TransactionState.COMPLETE_ABORT
                self.journal.append(transaction)
