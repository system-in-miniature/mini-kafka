"""Demonstrate that ``acks=1`` can confirm a leader-only write."""

from __future__ import annotations

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from minikafka import BrokerCluster, MiniKafkaConfig, TopicPartition


async def main() -> None:
    """Run the acknowledged-write-loss scenario through the Direct API."""
    with TemporaryDirectory(prefix="minikafka-acks-one-") as directory:
        config = MiniKafkaConfig(
            data_dir=Path(directory),
            broker_ids=(1, 2),
        )
        async with BrokerCluster.open(config) as cluster:
            await cluster.create_topic(
                "payments",
                partitions=1,
                replication_factor=2,
            )
            producer = cluster.producer(batch_size=1, acks=1)
            acknowledged = await producer.send(
                "payments",
                value=b"payment-confirmed",
            )
            partition = TopicPartition("payments", 0)

            print("1. Producer uses acks=1 (leader acknowledgement only).")
            print(f"   acknowledged offset: {acknowledged.offset}")
            print(
                "   consumer-visible end (HW): "
                f"{cluster.visible_end(partition)}"
            )
            print("   The write is acknowledged but has not reached the follower.")

            print("2. Simulate the leader failing before follower replication.")
            await cluster.promote(partition, broker_id=2)
            surviving = cluster.fetch(partition, 0, 10)
            print("   broker 2 is promoted; its log did not contain offset 0.")
            print(f"   records after failover: {len(surviving)}")

            print("3. Result: the acknowledged write was lost.")
            print(
                "   Kafka has the same acks=1 risk; use acks=all with an "
                "appropriate min.insync.replicas for stronger durability."
            )


if __name__ == "__main__":
    asyncio.run(main())
