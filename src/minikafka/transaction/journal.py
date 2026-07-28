from __future__ import annotations

import json
import os
import zlib
from pathlib import Path

from minikafka.core.metadata import TopicPartition
from minikafka.transaction.model import TransactionData, TransactionState


class TransactionJournal:
    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, transaction: TransactionData) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {
                "id": transaction.transaction_id,
                "state": transaction.state.value,
                "partitions": [
                    [tp.topic, tp.partition]
                    for tp in sorted(transaction.partitions)
                ],
                "first_offsets": [
                    [tp.topic, tp.partition, offset]
                    for tp, offset in sorted(transaction.first_offsets.items())
                ],
                "staged_offsets": {
                    group: [
                        [tp.topic, tp.partition, offset]
                        for tp, offset in sorted(offsets.items())
                    ]
                    for group, offsets in sorted(
                        transaction.staged_offsets.items()
                    )
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        with self.path.open("ab") as file:
            file.write(f"{crc:08x} ".encode() + payload + b"\n")
            file.flush()
            os.fsync(file.fileno())

    def recover(self) -> dict[str, TransactionData]:
        if not self.path.exists():
            return {}
        transactions: dict[str, TransactionData] = {}
        valid_end = 0
        with self.path.open("rb") as file:
            while line := file.readline():
                try:
                    crc_text, payload = line.rstrip(b"\n").split(b" ", 1)
                    if int(crc_text, 16) != zlib.crc32(payload) & 0xFFFFFFFF:
                        break
                    raw = json.loads(payload)
                    transaction = TransactionData(
                        transaction_id=raw["id"],
                        state=TransactionState(raw["state"]),
                        partitions={
                            TopicPartition(topic, partition)
                            for topic, partition in raw["partitions"]
                        },
                        first_offsets={
                            TopicPartition(topic, partition): offset
                            for topic, partition, offset in raw["first_offsets"]
                        },
                        staged_offsets={
                            group: {
                                TopicPartition(topic, partition): offset
                                for topic, partition, offset in offsets
                            }
                            for group, offsets in raw["staged_offsets"].items()
                        },
                    )
                except (ValueError, KeyError, TypeError, json.JSONDecodeError):
                    break
                transactions[transaction.transaction_id] = transaction
                valid_end = file.tell()
        if valid_end != self.path.stat().st_size:
            with self.path.open("r+b") as file:
                file.truncate(valid_end)
        return transactions
