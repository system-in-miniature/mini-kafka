from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from minikafka.core.batch import RecordBatch
from minikafka.errors import OutOfOrderSequence, ProducerFenced
from minikafka.replication.model import ProduceResult


@dataclass(frozen=True, slots=True)
class ProducerPartitionState:
    epoch: int
    last_sequence: int
    result: ProduceResult


class ProducerStateManager:
    def __init__(self, batches: tuple[RecordBatch, ...]) -> None:
        self._states: dict[int, ProducerPartitionState] = {}
        self._epochs: dict[int, int] = {}
        for batch in batches:
            if batch.producer_id >= 0:
                self.record(batch)

    def register_epoch(self, producer_id: int, epoch: int) -> None:
        current_epoch = self._epochs.get(producer_id, -1)
        if epoch < current_epoch:
            raise ProducerFenced(f"producer {producer_id} epoch {epoch}")
        self._epochs[producer_id] = epoch
        previous = self._states.get(producer_id)
        if previous is not None and epoch < previous.epoch:
            raise ProducerFenced(f"producer {producer_id} epoch {epoch}")
        if previous is not None and epoch > previous.epoch:
            self._states[producer_id] = ProducerPartitionState(
                epoch,
                -1,
                previous.result,
            )

    def validate(self, batch: RecordBatch) -> ProduceResult | None:
        if batch.producer_id < 0:
            return None
        current_epoch = self._epochs.get(batch.producer_id, -1)
        if batch.producer_epoch < current_epoch:
            raise ProducerFenced(
                f"producer {batch.producer_id} epoch "
                f"{batch.producer_epoch} is fenced by {current_epoch}"
            )
        previous = self._states.get(batch.producer_id)
        if previous is not None and batch.producer_epoch < previous.epoch:
            raise ProducerFenced(
                f"producer {batch.producer_id} epoch "
                f"{batch.producer_epoch} is fenced by {previous.epoch}"
            )
        if (
            previous is not None
            and batch.producer_epoch == previous.epoch
            and batch.last_sequence <= previous.last_sequence
        ):
            recorded = previous.result.batch
            if (
                batch.base_sequence == recorded.base_sequence
                and batch.last_sequence == recorded.last_sequence
            ):
                return previous.result
        expected = (
            0
            if previous is None or batch.producer_epoch > previous.epoch
            else previous.last_sequence + 1
        )
        if batch.base_sequence != expected:
            raise OutOfOrderSequence(expected, batch.base_sequence)
        return None

    def record(self, batch: RecordBatch) -> ProduceResult:
        result = ProduceResult(
            batch=batch,
            base_offset=batch.base_offset,
            next_offset=batch.next_offset,
        )
        self._states[batch.producer_id] = ProducerPartitionState(
            batch.producer_epoch,
            batch.last_sequence,
            result,
        )
        self._epochs[batch.producer_id] = max(
            self._epochs.get(batch.producer_id, -1),
            batch.producer_epoch,
        )
        return result


class ProducerIdentityStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._identities = self._load()

    def allocate(self, name: str) -> tuple[int, int]:
        current = self._identities.get(name)
        if current is None:
            producer_id, epoch = len(self._identities) + 1, 0
        else:
            producer_id, epoch = current[0], current[1] + 1
        self._identities[name] = (producer_id, epoch)
        self._save()
        return producer_id, epoch

    def _load(self) -> dict[str, tuple[int, int]]:
        if not self.path.exists():
            return {}
        raw = json.loads(self.path.read_text())
        return {
            name: (int(value[0]), int(value[1]))
            for name, value in raw.items()
        }

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        with temporary.open("w") as file:
            json.dump(self._identities, file, sort_keys=True)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, self.path)
