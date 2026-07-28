from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum

from minikafka.clock import Clock
from minikafka.consumer.assignor import round_robin_assign
from minikafka.consumer.offsets import OffsetStore
from minikafka.core.metadata import TopicPartition
from minikafka.errors import (
    IllegalGeneration,
    NotPartitionOwner,
    RebalanceInProgress,
    UnknownMember,
)


class GroupState(str, Enum):
    EMPTY = "empty"
    PREPARING_REBALANCE = "preparing_rebalance"
    STABLE = "stable"


@dataclass(slots=True)
class GroupMember:
    member_id: str
    subscriptions: set[str]
    deadline_ms: int


@dataclass(slots=True)
class Group:
    group_id: str
    generation: int = 0
    state: GroupState = GroupState.EMPTY
    members: dict[str, GroupMember] = field(default_factory=dict)
    assignment: dict[str, tuple[TopicPartition, ...]] = field(
        default_factory=dict
    )


@dataclass(frozen=True, slots=True)
class JoinResult:
    generation: int
    assignment: tuple[TopicPartition, ...]


class GroupCoordinator:
    def __init__(
        self,
        *,
        clock: Clock,
        offset_store: OffsetStore,
        topic_partitions: Callable[[], dict[str, tuple[int, ...]]],
        session_timeout_ms: int,
    ) -> None:
        self.clock = clock
        self.offset_store = offset_store
        self.topic_partitions = topic_partitions
        self.session_timeout_ms = session_timeout_ms
        self._groups: dict[str, Group] = {}

    async def join(
        self,
        group_id: str,
        member_id: str,
        subscriptions: Iterable[str],
    ) -> JoinResult:
        if not group_id or not member_id:
            raise ValueError("group and member IDs cannot be empty")
        requested = set(subscriptions)
        known = self.topic_partitions()
        missing = requested.difference(known)
        if missing:
            raise ValueError(f"unknown subscribed topics: {sorted(missing)}")
        group = self._groups.setdefault(group_id, Group(group_id))
        group.members[member_id] = GroupMember(
            member_id,
            requested,
            self.clock.now_ms() + self.session_timeout_ms,
        )
        self._rebalance(group)
        return self.assignment(group_id, member_id)

    async def leave(self, group_id: str, member_id: str) -> None:
        group = self._groups.get(group_id)
        if group is None or member_id not in group.members:
            return
        del group.members[member_id]
        self._rebalance(group)

    async def heartbeat(
        self,
        group_id: str,
        member_id: str,
        generation: int,
    ) -> None:
        group = self._require_group(group_id)
        self._validate_generation(group, generation)
        try:
            member = group.members[member_id]
        except KeyError as error:
            raise UnknownMember(member_id) from error
        member.deadline_ms = self.clock.now_ms() + self.session_timeout_ms

    async def expire_members(self) -> tuple[tuple[str, str], ...]:
        now = self.clock.now_ms()
        expired: list[tuple[str, str]] = []
        for group_id, group in sorted(self._groups.items()):
            removed = [
                member_id
                for member_id, member in group.members.items()
                if now > member.deadline_ms
            ]
            for member_id in removed:
                del group.members[member_id]
                expired.append((group_id, member_id))
            if removed:
                self._rebalance(group)
        return tuple(expired)

    def assignment(self, group_id: str, member_id: str) -> JoinResult:
        group = self._require_group(group_id)
        if member_id not in group.members:
            raise UnknownMember(member_id)
        if group.state is not GroupState.STABLE:
            raise RebalanceInProgress(group_id)
        return JoinResult(
            group.generation,
            group.assignment.get(member_id, ()),
        )

    def state(self, group_id: str) -> GroupState:
        return self._require_group(group_id).state

    async def commit(
        self,
        group_id: str,
        member_id: str,
        generation: int,
        offsets: dict[TopicPartition, int],
    ) -> None:
        group = self._require_group(group_id)
        if group.state is not GroupState.STABLE:
            raise RebalanceInProgress(group_id)
        self._validate_generation(group, generation)
        if member_id not in group.members:
            raise UnknownMember(member_id)
        owned = set(group.assignment.get(member_id, ()))
        if not set(offsets).issubset(owned):
            raise NotPartitionOwner(member_id)
        await self.offset_store.commit(group_id, offsets)

    def _require_group(self, group_id: str) -> Group:
        try:
            return self._groups[group_id]
        except KeyError as error:
            raise UnknownMember(f"unknown group {group_id}") from error

    @staticmethod
    def _validate_generation(group: Group, generation: int) -> None:
        if generation != group.generation:
            raise IllegalGeneration(generation, group.generation)

    def _rebalance(self, group: Group) -> None:
        group.state = GroupState.PREPARING_REBALANCE
        group.generation += 1
        if not group.members:
            group.assignment = {}
            group.state = GroupState.EMPTY
            return
        group.assignment = round_robin_assign(
            {
                member_id: member.subscriptions
                for member_id, member in group.members.items()
            },
            self.topic_partitions(),
        )
        group.state = GroupState.STABLE
