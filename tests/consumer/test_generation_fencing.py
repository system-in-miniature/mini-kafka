from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.consumer.group import GroupCoordinator
from minikafka.consumer.offsets import OffsetStore
from minikafka.core.metadata import TopicPartition
from minikafka.errors import IllegalGeneration, NotPartitionOwner


@pytest.mark.asyncio
async def test_old_generation_cannot_commit_after_rebalance(
    tmp_path: Path,
) -> None:
    clock = ManualClock()
    offsets = OffsetStore.open(tmp_path / "offsets.json")
    groups = GroupCoordinator(
        clock=clock,
        offset_store=offsets,
        topic_partitions=lambda: {"orders": (0, 1)},
        session_timeout_ms=100,
    )
    old = await groups.join("g", "a", {"orders"})
    await groups.join("g", "b", {"orders"})

    with pytest.raises(IllegalGeneration):
        await groups.commit(
            "g",
            "a",
            old.generation,
            {TopicPartition("orders", 0): 1},
        )


@pytest.mark.asyncio
async def test_member_cannot_commit_partition_it_does_not_own(
    tmp_path: Path,
) -> None:
    groups = GroupCoordinator(
        clock=ManualClock(),
        offset_store=OffsetStore.open(tmp_path / "offsets.json"),
        topic_partitions=lambda: {"orders": (0, 1)},
        session_timeout_ms=100,
    )
    await groups.join("g", "a", {"orders"})
    joined_b = await groups.join("g", "b", {"orders"})
    refreshed_a = groups.assignment("g", "a")

    with pytest.raises(NotPartitionOwner):
        await groups.commit(
            "g",
            "a",
            refreshed_a.generation,
            {joined_b.assignment[0]: 1},
        )
