from __future__ import annotations

from dataclasses import dataclass

from minikafka.log.partition_log import PartitionLog
from minikafka.replication.model import ReplicaState


@dataclass(slots=True)
class Replica:
    broker_id: int
    log: PartitionLog
    last_fetch_ms: int
    in_sync: bool = True

    @property
    def leo(self) -> int:
        return self.log.leo

    def state(self) -> ReplicaState:
        return ReplicaState(
            broker_id=self.broker_id,
            leo=self.leo,
            last_fetch_ms=self.last_fetch_ms,
            in_sync=self.in_sync,
        )
