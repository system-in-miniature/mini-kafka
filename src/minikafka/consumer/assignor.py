from __future__ import annotations

from minikafka.core.metadata import TopicPartition


def round_robin_assign(
    members: dict[str, set[str]],
    partitions: dict[str, tuple[int, ...]],
) -> dict[str, tuple[TopicPartition, ...]]:
    assigned: dict[str, list[TopicPartition]] = {
        member_id: [] for member_id in sorted(members)
    }
    cursor = 0
    for topic, topic_partitions in sorted(partitions.items()):
        eligible = sorted(
            member_id
            for member_id, subscriptions in members.items()
            if topic in subscriptions
        )
        if not eligible:
            continue
        for partition in sorted(topic_partitions):
            member_id = eligible[cursor % len(eligible)]
            assigned[member_id].append(TopicPartition(topic, partition))
            cursor += 1
    return {
        member_id: tuple(topic_partitions)
        for member_id, topic_partitions in assigned.items()
    }
