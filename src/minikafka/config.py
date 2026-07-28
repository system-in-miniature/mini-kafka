from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class MiniKafkaConfig:
    data_dir: Path
    segment_max_bytes: int = 1_048_576
    index_interval_bytes: int = 4_096

    def __post_init__(self) -> None:
        if self.segment_max_bytes <= 0:
            raise ValueError("segment_max_bytes must be positive")
        if self.index_interval_bytes <= 0:
            raise ValueError("index_interval_bytes must be positive")
