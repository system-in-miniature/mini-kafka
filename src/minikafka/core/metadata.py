from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

from minikafka.errors import StorageError

TOPIC_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


@dataclass(frozen=True, slots=True, order=True)
class TopicPartition:
    topic: str
    partition: int


@dataclass(frozen=True, slots=True)
class PartitionMetadata:
    topic: str
    partition: int
    replicas: tuple[int, ...]
    leader_id: int
    leader_epoch: int = 0


@dataclass(frozen=True, slots=True)
class TopicMetadata:
    name: str
    partitions: dict[int, PartitionMetadata]


def validate_topic_name(name: str) -> None:
    if (
        not name
        or name in {".", ".."}
        or len(name) > 249
        or TOPIC_PATTERN.fullmatch(name) is None
    ):
        raise ValueError(f"invalid topic name {name!r}")


def round_robin_replicas(
    broker_ids: tuple[int, ...],
    partitions: int,
    replication_factor: int,
) -> dict[int, tuple[int, ...]]:
    if partitions <= 0:
        raise ValueError("partitions must be positive")
    if replication_factor <= 0 or replication_factor > len(broker_ids):
        raise ValueError(
            "replication_factor must be positive and cannot exceed brokers"
        )
    return {
        partition: tuple(
            broker_ids[(partition + replica) % len(broker_ids)]
            for replica in range(replication_factor)
        )
        for partition in range(partitions)
    }


class MetadataStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, TopicMetadata]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text())
            topics: dict[str, TopicMetadata] = {}
            for name, topic_data in raw["topics"].items():
                partitions = {
                    int(number): PartitionMetadata(
                        topic=name,
                        partition=int(number),
                        replicas=tuple(partition_data["replicas"]),
                        leader_id=partition_data["leader_id"],
                        leader_epoch=partition_data["leader_epoch"],
                    )
                    for number, partition_data in topic_data["partitions"].items()
                }
                topics[name] = TopicMetadata(name, partitions)
            return topics
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise StorageError(f"invalid metadata file {self.path}") from error

    def save(self, topics: dict[str, TopicMetadata]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "topics": {
                name: {
                    "partitions": {
                        str(number): {
                            "replicas": list(metadata.replicas),
                            "leader_id": metadata.leader_id,
                            "leader_epoch": metadata.leader_epoch,
                        }
                        for number, metadata in sorted(topic.partitions.items())
                    }
                }
                for name, topic in sorted(topics.items())
            },
        }
        temporary = self.path.with_suffix(".tmp")
        with temporary.open("w") as file:
            json.dump(payload, file, sort_keys=True, separators=(",", ":"))
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, self.path)
        directory_fd = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
