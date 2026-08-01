# 自主重建

每个 Stage 都是一节可独立浏览的完整课：先理解当前问题、基本概念与必要性，再按机制板块连接相关文件和关键语句，最后用验证证据和自己的话完成理解闭环。

这是三种学习模式中的浏览器自主学习路径。按主题学习请进入[机制教程](../index.md)；需要 CLI 互动请查看 [Agent 带教使用教程](../agent-guide.md)。

如果希望在编辑器里聚焦当前增量，运行 `python -m journey.tools.build_journey study N`，再打开 `../MiniKafka-journey-workspace`。

| Stage | 主题 | 新增测试 | 教材章节 |
|---:|---|---:|---:|
| [01](stage-01.md) | 确定性基础 | 1 | [1](../tutorial/01-getting-started.md) |
| [02](stage-02.md) | 二进制安全 Record Batch | 1 | [2](../tutorial/02-the-log.md) |
| [03](stage-03.md) | 稀疏 Offset 查找 | 1 | [2](../tutorial/02-the-log.md) |
| [04](stage-04.md) | 可恢复日志段 | 2 | [2](../tutorial/02-the-log.md) |
| [05](stage-05.md) | 分段 Partition Log | 2 | [2](../tutorial/02-the-log.md) |
| [06](stage-06.md) | Topic 与直接管理接口 | 2 | [1](../tutorial/01-getting-started.md) |
| [07](stage-07.md) | 分区化 Producer Batching | 3 | [4](../tutorial/04-producer.md) |
| [08](stage-08.md) | Consumer Position 与重放 | 3 | [7](../tutorial/07-consumer-groups.md) |
| [09](stage-09.md) | Consumer Group 所有权 | 3 | [7](../tutorial/07-consumer-groups.md) |
| [10](stage-10.md) | 仅删除前缀的 Retention | 1 | [3](../tutorial/03-retention-compaction.md) |
| [11](stage-11.md) | 按 Key 的日志压实 | 3 | [3](../tutorial/03-retention-compaction.md) |
| [12](stage-12.md) | ISR 与 High Watermark | 3 | [5](../tutorial/05-replication-basics.md) |
| [13](stage-13.md) | 确认模式 | 2 | [6](../tutorial/06-isr-and-fencing.md) |
| [14](stage-14.md) | 晋升与 Epoch Fencing | 3 | [6](../tutorial/06-isr-and-fencing.md) |
| [15](stage-15.md) | 幂等 Producer 重试 | 2 | [4](../tutorial/04-producer.md) |
| [16](stage-16.md) | 事务化 Record 与 Offset | 4 | [8](../tutorial/08-transactions.md) |
| [17](stage-17.md) | 轻量 JSON TCP Adapter | 2 | [10](../tutorial/10-protocol-and-beyond.md) |
| [18](stage-18.md) | 生命周期与失败实验 | 2 | [9](../tutorial/09-delivery-semantics.md) |
| [19](stage-19.md) | ISR Rejoin 回归 | 1 | [6](../tutorial/06-isr-and-fencing.md) |
| [20](stage-20.md) | 跨机制领域闭环 | 5 | [9](../tutorial/09-delivery-semantics.md) |
