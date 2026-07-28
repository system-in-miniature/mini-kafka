from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.cluster import BrokerCluster
from minikafka.errors import StorageError
from minikafka.lifecycle import LifecycleState


@pytest.mark.asyncio
async def test_background_storage_failure_is_terminal(tmp_path: Path) -> None:
    cluster = BrokerCluster.open(
        MiniKafkaConfig(data_dir=tmp_path),
        clock=ManualClock(),
    )
    failure = StorageError("disk failed")
    cluster.failure_injector.fail_next_flush(failure)

    with pytest.raises(StorageError):
        await cluster.run_flush_cycle()

    assert cluster.state is LifecycleState.FAILED
    with pytest.raises(StorageError, match="disk failed"):
        await cluster.create_topic("later", 1, 1)
    await cluster.crash()
