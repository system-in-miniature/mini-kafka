from __future__ import annotations

from pathlib import Path

from minikafka.log.segment import Segment


def recover_active_segment(
    directory: Path,
    base_offset: int,
    index_interval_bytes: int,
) -> Segment:
    return Segment.open(
        directory,
        base_offset,
        index_interval_bytes,
        active=True,
    )
