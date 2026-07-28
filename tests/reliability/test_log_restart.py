from pathlib import Path

from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.log.partition_log import PartitionLog


def test_restart_preserves_leo_offsets_and_segments(tmp_path: Path) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        segment_max_bytes=100,
        index_interval_bytes=1,
    )
    directory = tmp_path / "events-0"
    log = PartitionLog.open(directory, config)
    for value in (b"x" * 40, b"y" * 40, b"z" * 40):
        log.append(RecordBatch.unassigned((Record(None, value, 1),)))
    bases = tuple(segment.base_offset for segment in log.segments)
    log.flush()
    log.close()

    reopened = PartitionLog.open(directory, config)

    assert reopened.leo == 3
    assert tuple(segment.base_offset for segment in reopened.segments) == bases
    assert [record.offset for record in reopened.fetch(0, 10)] == [0, 1, 2]
    reopened.close()
