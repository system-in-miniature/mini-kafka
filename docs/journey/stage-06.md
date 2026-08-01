# Stage 06 · Topics and direct administration

### Goal

Build topics and direct administration and explain its boundary from executable failure, runtime state, and the critical statement.

??? note "Deliverable files"
    - `src/minikafka/adapters/__init__.py`
    - `src/minikafka/adapters/direct.py`
    - `src/minikafka/config.py`
    - `src/minikafka/core/cluster.py`
    - `src/minikafka/core/metadata.py`
    - `src/minikafka/errors.py`
    - `tests/test_direct_cluster.py`
    - `tests/unit/test_metadata.py`

### The problem at this point

Durable logs are not yet a broker: callers need named topics, partition metadata, replica placement, and one semantic API.

### Test contract

#### See the failure first

Administration tests reject invalid names and replication factors, then reopen the cluster to prove metadata and log directories agree.

??? note "File diff: tests/test_direct_cluster.py"
    ```diff
    diff --git a/tests/test_direct_cluster.py b/tests/test_direct_cluster.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..3e02911804477e464ce1abbfab82b8712ff240f5
    --- /dev/null
    +++ b/tests/test_direct_cluster.py
    @@ -0,0 +1,53 @@
    +from pathlib import Path
    +
    +import pytest
    +
    +from minikafka.adapters.direct import DirectAdmin
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.core.metadata import TopicPartition
    +from minikafka.errors import TopicAlreadyExists, UnknownTopic
    +
    +
    +@pytest.mark.asyncio
    +async def test_create_topic_builds_partition_replica_logs(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    +
    +    async with BrokerCluster.open(config) as cluster:
    +        topic = await cluster.create_topic(
    +            "orders",
    +            partitions=3,
    +            replication_factor=2,
    +        )
    +
    +        assert tuple(topic.partitions) == (0, 1, 2)
    +        assert topic.partitions[0].replicas == (1, 2)
    +        assert cluster.replica_log(TopicPartition("orders", 0), 1).leo == 0
    +        assert cluster.replica_log(TopicPartition("orders", 0), 2).leo == 0
    +        with pytest.raises(TopicAlreadyExists):
    +            await cluster.create_topic("orders", 1, 1)
    +
    +
    +@pytest.mark.asyncio
    +async def test_direct_admin_delegates_to_cluster(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(7,))
    +
    +    async with BrokerCluster.open(config) as cluster:
    +        admin = DirectAdmin(cluster)
    +        created = await admin.create_topic("events", 1, 1)
    +
    +        assert await admin.describe_topic("events") == created
    +        with pytest.raises(UnknownTopic):
    +            await admin.describe_topic("missing")
    +
    +
    +@pytest.mark.asyncio
    +async def test_metadata_and_logs_survive_restart(tmp_path: Path) -> None:
    +    config = MiniKafkaConfig(data_dir=tmp_path, broker_ids=(1, 2))
    +    async with BrokerCluster.open(config) as cluster:
    +        await cluster.create_topic("events", 2, 2)
    +
    +    async with BrokerCluster.open(config) as reopened:
    +        topic = await reopened.describe_topic("events")
    +        assert topic.partitions[1].leader_id == 2
    +        assert reopened.replica_log(TopicPartition("events", 1), 1).leo == 0
    ```

**What this test locks**

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

**How it constructs the counterexample**

Administration tests reject invalid names and replication factors, then reopen the cluster to prove metadata and log directories agree.

**Key test statement**

```python
assert tuple(topic.partitions) == (0, 1, 2)
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

**What a failure means**

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

??? note "File diff: tests/unit/test_metadata.py"
    ```diff
    diff --git a/tests/unit/test_metadata.py b/tests/unit/test_metadata.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..a91a4301d74afea4c2f1d4e3ba68dc14695b8c9e
    --- /dev/null
    +++ b/tests/unit/test_metadata.py
    @@ -0,0 +1,47 @@
    +import pytest
    +
    +from minikafka.core.metadata import (
    +    TopicPartition,
    +    round_robin_replicas,
    +    validate_topic_name,
    +)
    +
    +
    +def test_topic_partition_is_orderable_and_hashable() -> None:
    +    values = {
    +        TopicPartition("b", 0),
    +        TopicPartition("a", 1),
    +        TopicPartition("a", 0),
    +    }
    +
    +    assert sorted(values) == [
    +        TopicPartition("a", 0),
    +        TopicPartition("a", 1),
    +        TopicPartition("b", 0),
    +    ]
    +
    +
    +def test_round_robin_replica_assignment_rotates_leaders() -> None:
    +    assignment = round_robin_replicas(
    +        broker_ids=(1, 2, 3),
    +        partitions=4,
    +        replication_factor=2,
    +    )
    +
    +    assert assignment == {
    +        0: (1, 2),
    +        1: (2, 3),
    +        2: (3, 1),
    +        3: (1, 2),
    +    }
    +
    +
    +@pytest.mark.parametrize("name", ("", ".", "..", "has/slash", "white space"))
    +def test_invalid_topic_names_are_rejected(name: str) -> None:
    +    with pytest.raises(ValueError, match="topic"):
    +        validate_topic_name(name)
    +
    +
    +def test_replication_factor_cannot_exceed_brokers() -> None:
    +    with pytest.raises(ValueError, match="replication_factor"):
    +        round_robin_replicas((1,), partitions=1, replication_factor=2)
    ```

**What this test locks**

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

**How it constructs the counterexample**

Administration tests reject invalid names and replication factors, then reopen the cluster to prove metadata and log directories agree.

**Key test statement**

```python
assert tuple(topic.partitions) == (0, 1, 2)
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

**What a failure means**

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage.

### Basic concepts

TopicPartition is the stable address of one ordered log. Metadata records leader and replicas. The Direct API is the semantic reference rather than a transport protocol.

### Why this mechanism is necessary

Durable logs are not yet a broker: callers need named topics, partition metadata, replica placement, and one semantic API. Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

Create-topic validates the request, assigns replicas deterministically, creates partition logs, persists metadata, and exposes the result through DirectAdmin.

### Mechanism blocks

#### Topics and direct administration mechanism

Create-topic validates the request, assigns replicas deterministically, creates partition logs, persists metadata, and exposes the result through DirectAdmin.

??? note "File diff: src/minikafka/adapters/direct.py"
    ```diff
    diff --git a/src/minikafka/adapters/direct.py b/src/minikafka/adapters/direct.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..6d7f4cbc859e3f07183c7ebb1e149b52de8a0b22
    --- /dev/null
    +++ b/src/minikafka/adapters/direct.py
    @@ -0,0 +1,24 @@
    +from __future__ import annotations
    +
    +from minikafka.core.cluster import BrokerCluster
    +from minikafka.core.metadata import TopicMetadata
    +
    +
    +class DirectAdmin:
    +    def __init__(self, cluster: BrokerCluster) -> None:
    +        self.cluster = cluster
    +
    +    async def create_topic(
    +        self,
    +        name: str,
    +        partitions: int,
    +        replication_factor: int,
    +    ) -> TopicMetadata:
    +        return await self.cluster.create_topic(
    +            name,
    +            partitions,
    +            replication_factor,
    +        )
    +
    +    async def describe_topic(self, name: str) -> TopicMetadata:
    +        return await self.cluster.describe_topic(name)
    ```

??? note "File diff: src/minikafka/config.py"
    ```diff
    diff --git a/src/minikafka/config.py b/src/minikafka/config.py
    index f4cad9a0d1f836971a271a3ec56c880c50225a6d..a209b81c134d32ee4258819e4ab9e595818a56a9 100644
    --- a/src/minikafka/config.py
    +++ b/src/minikafka/config.py
    @@ -7,6 +7,7 @@ from pathlib import Path
     @dataclass(frozen=True, slots=True)
     class MiniKafkaConfig:
         data_dir: Path
    +    broker_ids: tuple[int, ...] = (1,)
         segment_max_bytes: int = 1_048_576
         index_interval_bytes: int = 4_096

    @@ -15,3 +16,9 @@ class MiniKafkaConfig:
                 raise ValueError("segment_max_bytes must be positive")
             if self.index_interval_bytes <= 0:
                 raise ValueError("index_interval_bytes must be positive")
    +        if not self.broker_ids:
    +            raise ValueError("broker_ids cannot be empty")
    +        if len(set(self.broker_ids)) != len(self.broker_ids):
    +            raise ValueError("broker_ids must be unique")
    +        if any(broker_id < 0 for broker_id in self.broker_ids):
    +            raise ValueError("broker_ids cannot be negative")
    ```

??? note "File diff: src/minikafka/core/cluster.py"
    ```diff
    diff --git a/src/minikafka/core/cluster.py b/src/minikafka/core/cluster.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..549d85ae2a71742be015b44d4ebee1fb670ac5ee
    --- /dev/null
    +++ b/src/minikafka/core/cluster.py
    @@ -0,0 +1,156 @@
    +from __future__ import annotations
    +
    +from typing import Self
    +
    +from minikafka.config import MiniKafkaConfig
    +from minikafka.core.metadata import (
    +    MetadataStore,
    +    PartitionMetadata,
    +    TopicMetadata,
    +    TopicPartition,
    +    round_robin_replicas,
    +    validate_topic_name,
    +)
    +from minikafka.errors import (
    +    TopicAlreadyExists,
    +    UnknownPartition,
    +    UnknownTopic,
    +)
    +from minikafka.log.partition_log import PartitionLog
    +
    +
    +class BrokerCluster:
    +    def __init__(
    +        self,
    +        config: MiniKafkaConfig,
    +        metadata_store: MetadataStore,
    +        topics: dict[str, TopicMetadata],
    +        logs: dict[tuple[TopicPartition, int], PartitionLog],
    +    ) -> None:
    +        self.config = config
    +        self._metadata_store = metadata_store
    +        self._topics = topics
    +        self._logs = logs
    +        self._closed = False
    +
    +    @classmethod
    +    def open(cls, config: MiniKafkaConfig) -> BrokerCluster:
    +        metadata_store = MetadataStore(config.data_dir / "metadata.json")
    +        topics = metadata_store.load()
    +        logs: dict[tuple[TopicPartition, int], PartitionLog] = {}
    +        try:
    +            for topic in topics.values():
    +                for partition in topic.partitions.values():
    +                    tp = TopicPartition(topic.name, partition.partition)
    +                    for broker_id in partition.replicas:
    +                        logs[(tp, broker_id)] = PartitionLog.open(
    +                            config.data_dir
    +                            / f"broker-{broker_id}"
    +                            / f"{topic.name}-{partition.partition}",
    +                            config,
    +                            leader_epoch=partition.leader_epoch,
    +                        )
    +        except BaseException:
    +            for log in logs.values():
    +                log.close()
    +            raise
    +        return cls(config, metadata_store, topics, logs)
    +
    +    async def __aenter__(self) -> Self:
    +        return self
    +
    +    async def __aexit__(self, *_: object) -> None:
    +        await self.close()
    +
    +    async def create_topic(
    +        self,
    +        name: str,
    +        partitions: int,
    +        replication_factor: int,
    +    ) -> TopicMetadata:
    +        self._ensure_open()
    +        validate_topic_name(name)
    +        if name in self._topics:
    +            raise TopicAlreadyExists(name)
    +        assignment = round_robin_replicas(
    +            self.config.broker_ids,
    +            partitions,
    +            replication_factor,
    +        )
    +        partition_metadata = {
    +            partition: PartitionMetadata(
    +                topic=name,
    +                partition=partition,
    +                replicas=replicas,
    +                leader_id=replicas[0],
    +            )
    +            for partition, replicas in assignment.items()
    +        }
    +        topic = TopicMetadata(name, partition_metadata)
    +        opened: dict[tuple[TopicPartition, int], PartitionLog] = {}
    +        try:
    +            for partition, metadata in partition_metadata.items():
    +                tp = TopicPartition(name, partition)
    +                for broker_id in metadata.replicas:
    +                    opened[(tp, broker_id)] = PartitionLog.open(
    +                        self.config.data_dir
    +                        / f"broker-{broker_id}"
    +                        / f"{name}-{partition}",
    +                        self.config,
    +                    )
    +            prospective = dict(self._topics)
    +            prospective[name] = topic
    +            self._metadata_store.save(prospective)
    +        except BaseException:
    +            for log in opened.values():
    +                log.close()
    +            raise
    +        self._logs.update(opened)
    +        self._topics[name] = topic
    +        return topic
    +
    +    async def describe_topic(self, name: str) -> TopicMetadata:
    +        self._ensure_open()
    +        try:
    +            return self._topics[name]
    +        except KeyError as error:
    +            raise UnknownTopic(name) from error
    +
    +    def topic(self, name: str) -> TopicMetadata:
    +        self._ensure_open()
    +        try:
    +            return self._topics[name]
    +        except KeyError as error:
    +            raise UnknownTopic(name) from error
    +
    +    def partition_metadata(self, tp: TopicPartition) -> PartitionMetadata:
    +        topic = self.topic(tp.topic)
    +        try:
    +            return topic.partitions[tp.partition]
    +        except KeyError as error:
    +            raise UnknownPartition(f"{tp.topic}-{tp.partition}") from error
    +
    +    def replica_log(self, tp: TopicPartition, broker_id: int) -> PartitionLog:
    +        self.partition_metadata(tp)
    +        try:
    +            return self._logs[(tp, broker_id)]
    +        except KeyError as error:
    +            raise UnknownPartition(
    +                f"broker {broker_id} has no replica for {tp}"
    +            ) from error
    +
    +    def leader_log(self, tp: TopicPartition) -> PartitionLog:
    +        metadata = self.partition_metadata(tp)
    +        return self.replica_log(tp, metadata.leader_id)
    +
    +    async def close(self) -> None:
    +        if self._closed:
    +            return
    +        for log in self._logs.values():
    +            log.flush()
    +            log.close()
    +        self._closed = True
    +
    +    def _ensure_open(self) -> None:
    +        if self._closed:
    +            raise RuntimeError("cluster is closed")
    ```

??? note "File diff: src/minikafka/core/metadata.py"
    ```diff
    diff --git a/src/minikafka/core/metadata.py b/src/minikafka/core/metadata.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..e708df0730183b5950157f5d92d1499740ed9e62
    --- /dev/null
    +++ b/src/minikafka/core/metadata.py
    @@ -0,0 +1,119 @@
    +from __future__ import annotations
    +
    +import json
    +import os
    +import re
    +from dataclasses import dataclass
    +from pathlib import Path
    +
    +from minikafka.errors import StorageError
    +
    +TOPIC_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
    +
    +
    +@dataclass(frozen=True, slots=True, order=True)
    +class TopicPartition:
    +    topic: str
    +    partition: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class PartitionMetadata:
    +    topic: str
    +    partition: int
    +    replicas: tuple[int, ...]
    +    leader_id: int
    +    leader_epoch: int = 0
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class TopicMetadata:
    +    name: str
    +    partitions: dict[int, PartitionMetadata]
    +
    +
    +def validate_topic_name(name: str) -> None:
    +    if (
    +        not name
    +        or name in {".", ".."}
    +        or len(name) > 249
    +        or TOPIC_PATTERN.fullmatch(name) is None
    +    ):
    +        raise ValueError(f"invalid topic name {name!r}")
    +
    +
    +def round_robin_replicas(
    +    broker_ids: tuple[int, ...],
    +    partitions: int,
    +    replication_factor: int,
    +) -> dict[int, tuple[int, ...]]:
    +    if partitions <= 0:
    +        raise ValueError("partitions must be positive")
    +    if replication_factor <= 0 or replication_factor > len(broker_ids):
    +        raise ValueError(
    +            "replication_factor must be positive and cannot exceed brokers"
    +        )
    +    return {
    +        partition: tuple(
    +            broker_ids[(partition + replica) % len(broker_ids)]
    +            for replica in range(replication_factor)
    +        )
    +        for partition in range(partitions)
    +    }
    +
    +
    +class MetadataStore:
    +    def __init__(self, path: Path) -> None:
    +        self.path = path
    +
    +    def load(self) -> dict[str, TopicMetadata]:
    +        if not self.path.exists():
    +            return {}
    +        try:
    +            raw = json.loads(self.path.read_text())
    +            topics: dict[str, TopicMetadata] = {}
    +            for name, topic_data in raw["topics"].items():
    +                partitions = {
    +                    int(number): PartitionMetadata(
    +                        topic=name,
    +                        partition=int(number),
    +                        replicas=tuple(partition_data["replicas"]),
    +                        leader_id=partition_data["leader_id"],
    +                        leader_epoch=partition_data["leader_epoch"],
    +                    )
    +                    for number, partition_data in topic_data["partitions"].items()
    +                }
    +                topics[name] = TopicMetadata(name, partitions)
    +            return topics
    +        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
    +            raise StorageError(f"invalid metadata file {self.path}") from error
    +
    +    def save(self, topics: dict[str, TopicMetadata]) -> None:
    +        self.path.parent.mkdir(parents=True, exist_ok=True)
    +        payload = {
    +            "version": 1,
    +            "topics": {
    +                name: {
    +                    "partitions": {
    +                        str(number): {
    +                            "replicas": list(metadata.replicas),
    +                            "leader_id": metadata.leader_id,
    +                            "leader_epoch": metadata.leader_epoch,
    +                        }
    +                        for number, metadata in sorted(topic.partitions.items())
    +                    }
    +                }
    +                for name, topic in sorted(topics.items())
    +            },
    +        }
    +        temporary = self.path.with_suffix(".tmp")
    +        with temporary.open("w") as file:
    +            json.dump(payload, file, sort_keys=True, separators=(",", ":"))
    +            file.flush()
    +            os.fsync(file.fileno())
    +        os.replace(temporary, self.path)
    +        directory_fd = os.open(self.path.parent, os.O_RDONLY)
    +        try:
    +            os.fsync(directory_fd)
    +        finally:
    +            os.close(directory_fd)
    ```

??? note "File diff: src/minikafka/errors.py"
    ```diff
    diff --git a/src/minikafka/errors.py b/src/minikafka/errors.py
    index ce404ed7130bb07f19525a5bc7b592350b81765a..0c17241801131b7552aaa3eb229182fe3b570cdd 100644
    --- a/src/minikafka/errors.py
    +++ b/src/minikafka/errors.py
    @@ -31,3 +31,15 @@ class CorruptIndex(MiniKafkaError):

     class StorageError(MiniKafkaError):
         code = "STORAGE_ERROR"
    +
    +
    +class TopicAlreadyExists(MiniKafkaError):
    +    code = "TOPIC_ALREADY_EXISTS"
    +
    +
    +class UnknownTopic(MiniKafkaError):
    +    code = "UNKNOWN_TOPIC"
    +
    +
    +class UnknownPartition(MiniKafkaError):
    +    code = "UNKNOWN_PARTITION"
    ```

**What it is and why it appears**

TopicPartition is the stable address of one ordered log. Metadata records leader and replicas. The Direct API is the semantic reference rather than a transport protocol.

**Runtime role**

Create-topic validates the request, assigns replicas deterministically, creates partition logs, persists metadata, and exposes the result through DirectAdmin.

**Statement understanding**

Metadata must be persisted with the same partition identity used to locate logs; otherwise restart reconstructs a different broker topology.

#### Package and project support

Keep package exports, dependencies, and the test environment reproducible.

??? note "Supporting file diffs (1 file)"
    **`src/minikafka/adapters/__init__.py`**

    ```diff
    diff --git a/src/minikafka/adapters/__init__.py b/src/minikafka/adapters/__init__.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..af1d834ca61b0358c240fcfd49a5de542295153a
    --- /dev/null
    +++ b/src/minikafka/adapters/__init__.py
    @@ -0,0 +1 @@
    +"""Transport adapters for MiniKafka."""
    ```


### Verification evidence

Run `uv run pytest -q $(cat journey/stages/06-topic-administration/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

Metadata must be persisted with the same partition identity used to locate logs; otherwise restart reconstructs a different broker topology.

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter 1](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/01-getting-started.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/06-topic-administration/stage.patch)
