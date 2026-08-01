# Stage 19 · ISR Rejoin 回归

### 目标

实现ISR Rejoin 回归，并能从可执行失败、运行时状态与关键语句解释其边界。

??? note "交付文件"
    - `src/minikafka/replication/replica_set.py`
    - `tests/replication/test_promotion.py`

### 当前遇到的问题

Follower 可能接近 Leader LEO 却仍落后于已提交 High Watermark，因此只看 LEO 的 Rejoin 会接纳不安全的选举候选。

### 测试契约

#### 先看会坏在哪里

回归测试构造落后 HW 的 Replica，尝试让它 Rejoin，再尝试 Promotion；旧谓词会让两步都成功。

??? note "文件差异：tests/replication/test_promotion.py"
    ```diff
    diff --git a/tests/replication/test_promotion.py b/tests/replication/test_promotion.py
    index 4ff2b19397fcf372f8bda749f6b7375832ae55bf..11407d3270a31329ad60b0d41d28e1afca781352 100644
    --- a/tests/replication/test_promotion.py
    +++ b/tests/replication/test_promotion.py
    @@ -29,6 +29,32 @@ async def test_only_isr_replica_can_be_promoted(tmp_path: Path) -> None:
                 await cluster.promote(tp, broker_id=2)


    +@pytest.mark.asyncio
    +async def test_replica_behind_high_watermark_cannot_reenter_isr(
    +    tmp_path: Path,
    +) -> None:
    +    config = MiniKafkaConfig(
    +        data_dir=tmp_path,
    +        broker_ids=(1, 2),
    +        replica_lag_max_offsets=10,
    +    )
    +    async with BrokerCluster.open(config, clock=ManualClock()) as cluster:
    +        await cluster.create_topic("events", 1, 2)
    +        tp = TopicPartition("events", 0)
    +        replica_set = cluster.replica_set(tp)
    +        await replica_set.append(batch(b"committed"), AckMode.LEADER)
    +        await replica_set.fetch_followers_once()
    +        assert replica_set.high_watermark == 1
    +        replica_set.remove_from_isr(2)
    +        replica_set.replicas[2].log.truncate_to(0)
    +
    +        replica_set.refresh_isr()
    +
    +        assert 2 not in replica_set.isr
    +        with pytest.raises(NotInSyncReplica):
    +            await cluster.promote(tp, broker_id=2)
    +
    +
     @pytest.mark.asyncio
     async def test_promotion_increments_epoch_and_fences_old_requests(
         tmp_path: Path,
    ```

**测试锁定什么**

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

**如何构造反例**

回归测试构造落后 HW 的 Replica，尝试让它 Rejoin，再尝试 Promotion；旧谓词会让两步都成功。

**关键测试语句**

```python
assert replica_set.high_watermark == 1
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

**失败意味着什么**

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。

### 基本概念

ISR Membership 是安全承诺而非新鲜度提示。Rejoin Replica 必须拥有完整 Commit Prefix，才能参与 Ack 或成为 Leader。

### 为什么需要这个机制

Follower 可能接近 Leader LEO 却仍落后于已提交 High Watermark，因此只看 LEO 的 Rejoin 会接纳不安全的选举候选。 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

Membership Refresh 同时比较 Follower LEO、Lag Policy 与 HW；只有到达 HW 的 Replica 才能重新进入 Ack 与 Election 集合。

### 机制板块

#### ISR Rejoin 回归机制

Membership Refresh 同时比较 Follower LEO、Lag Policy 与 HW；只有到达 HW 的 Replica 才能重新进入 Ack 与 Election 集合。

??? note "文件差异：src/minikafka/replication/replica_set.py"
    ```diff
    diff --git a/src/minikafka/replication/replica_set.py b/src/minikafka/replication/replica_set.py
    index 51e1a0002e30d9a7f220918f61ce0477590eaebc..97c678d2d3e18568bfb96916463e64718d0bf8b6 100644
    --- a/src/minikafka/replication/replica_set.py
    +++ b/src/minikafka/replication/replica_set.py
    @@ -199,7 +199,8 @@ class PartitionReplicaSet:
                     now - follower.last_fetch_ms
                     <= self.config.replica_lag_time_ms
                 )
    -            if within_offset and within_time:
    +            has_committed_prefix = follower.leo >= self.high_watermark
    +            if within_offset and within_time and has_committed_prefix:
                     next_isr.add(follower.broker_id)
             self._isr = next_isr
             for broker_id, replica in self.replicas.items():
    ```

**是什么，为什么现在需要**

ISR Membership 是安全承诺而非新鲜度提示。Rejoin Replica 必须拥有完整 Commit Prefix，才能参与 Ack 或成为 Leader。

**在运行时做什么**

Membership Refresh 同时比较 Follower LEO、Lag Policy 与 HW；只有到达 HW 的 Replica 才能重新进入 Ack 与 Election 集合。

**关键语句理解**

`leo >= high_watermark` 这个合取条件是缺失的安全门：没有它，Leader Progress 变化会让不完整 Replica 看似已追上。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/19-isr-regression/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

`leo >= high_watermark` 这个合取条件是缺失的安全门：没有它，Leader Progress 变化会让不完整 Replica 看似已追上。

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 6 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/06-isr-and-fencing.md)

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-kafka/blob/main/journey/stages/19-isr-regression/stage.patch)
