"""Observe assignment changes as members join and leave a consumer group."""

from __future__ import annotations

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition


def _format(partitions: tuple[TopicPartition, ...]) -> str:
    return ", ".join(f"{item.topic}-{item.partition}" for item in partitions)


async def main() -> None:
    """Run a join/refresh/leave rebalance through the public Consumer API."""
    with TemporaryDirectory(prefix="minikafka-rebalance-") as directory:
        config = MiniKafkaConfig(data_dir=Path(directory))
        async with BrokerCluster.open(config) as cluster:
            await cluster.create_topic(
                "orders",
                partitions=4,
                replication_factor=1,
            )
            first = cluster.consumer(group_id="workers")
            second = cluster.consumer(group_id="workers")

            print("1. First member joins and initially owns every partition.")
            await first.subscribe(("orders",))
            print(f"   member 1: {_format(first.assignment)}")

            print("2. Second member joins, triggering a new generation.")
            await second.subscribe(("orders",))
            print(f"   member 2: {_format(second.assignment)}")
            print(
                "   member 1 local view before refresh: "
                f"{_format(first.assignment)}"
            )
            print(
                "   MiniKafka deliberately has no revoke barrier: the old "
                "local assignment remains until refresh_assignment()."
            )

            await first.refresh_assignment()
            print("3. Member 1 refreshes and observes the stable assignment.")
            print(f"   member 1: {_format(first.assignment)}")
            print(f"   member 2: {_format(second.assignment)}")
            print(
                "   overlap after refresh: "
                f"{bool(set(first.assignment) & set(second.assignment))}"
            )

            print("4. Member 2 leaves; the remaining member owns all partitions.")
            await second.close()
            await first.refresh_assignment()
            print(f"   member 1: {_format(first.assignment)}")
            print(
                "   Real Kafka coordinates JoinGroup/SyncGroup and revocation; "
                "this mini coordinator completes rebalance synchronously."
            )


if __name__ == "__main__":
    asyncio.run(main())
