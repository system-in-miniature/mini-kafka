from pathlib import Path

import pytest

from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.consumer.group import GroupCoordinator, GroupState
from minikafka.consumer.offsets import OffsetStore
from minikafka.core.cluster import BrokerCluster


def coordinator(tmp_path: Path, clock: ManualClock) -> GroupCoordinator:
    return GroupCoordinator(
        clock=clock,
        offset_store=OffsetStore.open(tmp_path / "offsets.json"),
        topic_partitions=lambda: {"orders": (0, 1, 2, 3)},
        session_timeout_ms=100,
    )


@pytest.mark.asyncio
async def test_join_and_leave_rebalance_partition_ownership(tmp_path: Path) -> None:
    clock = ManualClock()
    groups = coordinator(tmp_path, clock)
    first = await groups.join("g", "a", {"orders"})
    assert len(first.assignment) == 4

    second = await groups.join("g", "b", {"orders"})
    refreshed = groups.assignment("g", "a")

    assert second.generation == first.generation + 1
    assert len(second.assignment) == 2
    assert len(refreshed.assignment) == 2
    assert set(second.assignment).isdisjoint(refreshed.assignment)

    await groups.leave("g", "b")
    assert len(groups.assignment("g", "a").assignment) == 4
    assert groups.state("g") is GroupState.STABLE


@pytest.mark.asyncio
async def test_heartbeat_keeps_member_alive_until_new_deadline(
    tmp_path: Path,
) -> None:
    clock = ManualClock()
    groups = coordinator(tmp_path, clock)
    joined = await groups.join("g", "a", {"orders"})
    clock.advance_ms(80)
    await groups.heartbeat("g", "a", joined.generation)
    clock.advance_ms(80)

    assert await groups.expire_members() == ()
    assert groups.assignment("g", "a").assignment


@pytest.mark.asyncio
async def test_expiry_removes_member_and_rebalances(tmp_path: Path) -> None:
    clock = ManualClock()
    groups = coordinator(tmp_path, clock)
    await groups.join("g", "a", {"orders"})
    joined_b = await groups.join("g", "b", {"orders"})
    await groups.heartbeat("g", "b", joined_b.generation)
    clock.advance_ms(101)

    expired = await groups.expire_members()

    assert set(expired) == {("g", "a"), ("g", "b")}
    assert groups.state("g") is GroupState.EMPTY


@pytest.mark.asyncio
async def test_cluster_consumers_refresh_group_assignment(tmp_path: Path) -> None:
    config = MiniKafkaConfig(data_dir=tmp_path)
    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
        await cluster.create_topic("orders", 4, 1)
        first = cluster.consumer(group_id="workers")
        second = cluster.consumer(group_id="workers")

        await first.subscribe(("orders",))
        await second.subscribe(("orders",))
        await first.refresh_assignment()

        assert len(first.assignment) == 2
        assert len(second.assignment) == 2
        assert set(first.assignment).isdisjoint(second.assignment)
