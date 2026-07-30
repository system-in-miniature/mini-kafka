> **语言**: [English](../behavior-matrix.md) | 简体中文

# MiniKafka 行为矩阵

此矩阵将每项承诺的语义与可执行证据绑定。

| Semantic | Evidence |
| --- | --- |
| Binary-safe batches and CRC | `tests/unit/test_batch_codec.py` |
| Sparse offset lookup | `tests/unit/test_offset_index.py` |
| Tail repair and closed-segment corruption | `tests/log/test_recovery.py` |
| Offset replay across segments | `tests/log/test_partition_log.py` |
| Keyed/sticky partitioning and batching | `tests/producer/test_ordering.py`, `tests/producer/test_batching.py` |
| Position, commit, rewind and restart | `tests/consumer/test_positions.py`, `tests/reliability/test_offset_restart.py` |
| Single group owner and generation fencing | `tests/consumer/test_group_rebalance.py`, `tests/consumer/test_generation_fencing.py` |
| Segment retention | `tests/log/test_retention.py` |
| Key compaction and failure-aware directory replacement | `tests/log/test_compaction.py`, `tests/reliability/test_compaction_swap.py` |
| Follower fetch, ISR and high watermark | `tests/replication/test_follower_fetch.py`, `tests/replication/test_high_watermark.py` |
| Ack modes and acknowledged-write loss | `tests/replication/test_ack_modes.py`, `tests/reliability/test_lost_acked_write.py` |
| Promotion fencing and divergent-tail truncation | `test_old_leader_truncates_uncommitted_tail`, `tests/replication/test_promotion.py` |
| Idempotent retry and restart rebuild | `test_exact_retry_returns_original_offsets`, `tests/reliability/test_producer_state_restart.py` |
| Transaction isolation and abort | `tests/transaction/test_visibility.py`, `tests/transaction/test_abort.py` |
| Atomic output and input offsets | `test_output_and_input_offset_publish_together` |
| Journal recovery | `tests/reliability/test_transaction_restart.py` |
| Direct/TCP parity | `tests/adapters/test_direct_tcp_parity.py` |
| Graceful close, crash, terminal failure | `tests/reliability/test_shutdown.py`, `tests/reliability/test_background_failure.py` |
| Cross-feature closure and restart | `tests/test_final_acceptance.py` |

JSON/TCP 适配器只证明边界转换。直接 API 仍然是核心测试和故障语义测试的
权威路径。
