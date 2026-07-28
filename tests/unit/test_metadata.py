import pytest

from minikafka.core.metadata import (
    TopicPartition,
    round_robin_replicas,
    validate_topic_name,
)


def test_topic_partition_is_orderable_and_hashable() -> None:
    values = {
        TopicPartition("b", 0),
        TopicPartition("a", 1),
        TopicPartition("a", 0),
    }

    assert sorted(values) == [
        TopicPartition("a", 0),
        TopicPartition("a", 1),
        TopicPartition("b", 0),
    ]


def test_round_robin_replica_assignment_rotates_leaders() -> None:
    assignment = round_robin_replicas(
        broker_ids=(1, 2, 3),
        partitions=4,
        replication_factor=2,
    )

    assert assignment == {
        0: (1, 2),
        1: (2, 3),
        2: (3, 1),
        3: (1, 2),
    }


@pytest.mark.parametrize("name", ("", ".", "..", "has/slash", "white space"))
def test_invalid_topic_names_are_rejected(name: str) -> None:
    with pytest.raises(ValueError, match="topic"):
        validate_topic_name(name)


def test_replication_factor_cannot_exceed_brokers() -> None:
    with pytest.raises(ValueError, match="replication_factor"):
        round_robin_replicas((1,), partitions=1, replication_factor=2)
