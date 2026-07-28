from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class MiniKafkaConfig:
    data_dir: Path
    broker_ids: tuple[int, ...] = (1,)
    segment_max_bytes: int = 1_048_576
    index_interval_bytes: int = 4_096
    group_session_timeout_ms: int = 10_000

    def __post_init__(self) -> None:
        if self.segment_max_bytes <= 0:
            raise ValueError("segment_max_bytes must be positive")
        if self.index_interval_bytes <= 0:
            raise ValueError("index_interval_bytes must be positive")
        if not self.broker_ids:
            raise ValueError("broker_ids cannot be empty")
        if len(set(self.broker_ids)) != len(self.broker_ids):
            raise ValueError("broker_ids must be unique")
        if any(broker_id < 0 for broker_id in self.broker_ids):
            raise ValueError("broker_ids cannot be negative")
        if self.group_session_timeout_ms <= 0:
            raise ValueError("group_session_timeout_ms must be positive")
