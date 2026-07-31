# MiniKafka 行为矩阵

此矩阵将每项承诺的语义与可执行证据绑定。

| 语义（Semantic） | 证据（Evidence） |
| --- | --- |
| 二进制安全批次（binary-safe batches）与 CRC | `tests/unit/test_batch_codec.py` |
| 稀疏偏移量查找（sparse offset lookup） | `tests/unit/test_offset_index.py` |
| 尾部修复与已关闭分段损坏 | `tests/log/test_recovery.py` |
| 跨分段偏移量重放 | `tests/log/test_partition_log.py` |
| 按键分区、粘性分区与批处理 | `tests/producer/test_ordering.py`, `tests/producer/test_batching.py` |
| 位置、提交、回退与重启 | `tests/consumer/test_positions.py`, `tests/reliability/test_offset_restart.py` |
| 单一消费组所有者与世代隔离（generation fencing） | `tests/consumer/test_group_rebalance.py`, `tests/consumer/test_generation_fencing.py` |
| 分段保留 | `tests/log/test_retention.py` |
| 按键压实（key compaction）与感知故障的目录替换 | `tests/log/test_compaction.py`, `tests/reliability/test_compaction_swap.py` |
| 跟随者拉取、同步副本集合（ISR）与高水位线（high watermark） | `tests/replication/test_follower_fetch.py`, `tests/replication/test_high_watermark.py` |
| 确认模式（ack modes）与已确认写入丢失 | `tests/replication/test_ack_modes.py`, `tests/reliability/test_lost_acked_write.py` |
| 晋升隔离与分歧尾部截断 | `test_old_leader_truncates_uncommitted_tail`, `tests/replication/test_promotion.py` |
| 幂等重试与重启后重建 | `test_exact_retry_returns_original_offsets`, `tests/reliability/test_producer_state_restart.py` |
| 事务隔离与中止 | `tests/transaction/test_visibility.py`, `tests/transaction/test_abort.py` |
| 输出与输入偏移量的原子发布 | `test_output_and_input_offset_publish_together` |
| 日志（journal）恢复 | `tests/reliability/test_transaction_restart.py` |
| 直接调用/TCP 一致性 | `tests/adapters/test_direct_tcp_parity.py` |
| 优雅关闭、崩溃与终止性故障 | `tests/reliability/test_shutdown.py`, `tests/reliability/test_background_failure.py` |
| 跨功能闭环与重启 | `tests/test_final_acceptance.py` |

JSON/TCP 适配器只证明边界转换。直接 API 仍然是核心测试和故障语义测试的
权威路径。

## 下一步

继续阅读[教程目录](tutorial/index.md)，按章节学习这些机制。
