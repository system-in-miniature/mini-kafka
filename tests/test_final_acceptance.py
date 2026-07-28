import asyncio
from pathlib import Path

import pytest

from minikafka import (
    AckMode,
    BrokerCluster,
    IsolationLevel,
    MiniKafkaConfig,
    TopicPartition,
)
from minikafka.clock import ManualClock


@pytest.mark.asyncio
async def test_domain_closure_survives_rebalance_failover_and_restart(
    tmp_path: Path,
) -> None:
    config = MiniKafkaConfig(
        data_dir=tmp_path,
        broker_ids=(1, 2),
        min_insync_replicas=2,
        group_session_timeout_ms=10,
    )
    clock = ManualClock()
    async with BrokerCluster.open(config, clock=clock) as cluster:
        await cluster.create_topic("orders", 2, 2)
        producer = cluster.producer(
            acks=AckMode.ALL,
            idempotent=True,
            batch_size=1,
        )
        pending = producer.send(
            "orders",
            key=b"user-42",
            value=b"created",
        )
        await asyncio.sleep(0)
        await cluster.replicate_all_once()
        metadata = await pending
        tp = TopicPartition("orders", metadata.partition)

        first = cluster.consumer(group_id="workers")
        await first.subscribe(("orders",))
        records = await first.poll(10)
        assert [record.value for record in records] == [b"created"]
        await first.commit()

        clock.advance_ms(11)
        assert await cluster.expire_group_members()
        second = cluster.consumer(group_id="workers")
        await second.subscribe(("orders",))
        assert tp in second.assignment
        follower_id = next(
            broker_id
            for broker_id in cluster.partition_metadata(tp).replicas
            if broker_id != cluster.partition_metadata(tp).leader_id
        )
        await cluster.promote(tp, follower_id)

    async with BrokerCluster.open(config, clock=ManualClock()) as reopened:
        assert await reopened.offsets.get("workers", tp) == (
            records[-1].offset + 1
        )
        assert reopened.partition_metadata(tp).leader_id == follower_id


def test_readme_states_adapter_and_course_boundaries() -> None:
    text = Path("README.md").read_text()
    assert "not Kafka wire-protocol compatible" in text
    assert "Course material is separate" in text
    assert "Direct API" in text


def test_behavior_matrix_names_executable_evidence() -> None:
    text = Path("docs/behavior-matrix.md").read_text()
    assert "test_exact_retry_returns_original_offsets" in text
    assert "test_old_leader_truncates_uncommitted_tail" in text
    assert "test_output_and_input_offset_publish_together" in text


def test_public_api_exports_semantic_types() -> None:
    assert AckMode.ALL.value == "all"
    assert IsolationLevel.READ_COMMITTED.value == "read_committed"
