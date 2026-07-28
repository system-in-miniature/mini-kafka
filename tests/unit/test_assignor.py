from minikafka.consumer.assignor import round_robin_assign
from minikafka.core.metadata import TopicPartition


def test_round_robin_assignor_has_single_owner() -> None:
    assignment = round_robin_assign(
        members={"a": {"orders"}, "b": {"orders"}},
        partitions={"orders": (0, 1, 2, 3)},
    )

    assert assignment["a"] == (
        TopicPartition("orders", 0),
        TopicPartition("orders", 2),
    )
    assert assignment["b"] == (
        TopicPartition("orders", 1),
        TopicPartition("orders", 3),
    )
    assert set(assignment["a"]).isdisjoint(assignment["b"])


def test_assignor_respects_topic_subscriptions() -> None:
    assignment = round_robin_assign(
        members={"orders-only": {"orders"}, "all": {"orders", "payments"}},
        partitions={"orders": (0, 1), "payments": (0,)},
    )

    assert TopicPartition("payments", 0) not in assignment["orders-only"]
    assert TopicPartition("payments", 0) in assignment["all"]
