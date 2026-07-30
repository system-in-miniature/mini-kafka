# 第 2 章 · 日志：一切的地基

Kafka 常被介绍成消息中间件，但它的持久化抽象是有序、只追加的分区日志。
生产者追加，副本复制相同的有序 batch，消费者给出 offset 并重放。
MiniKafka 用三层对象把地基展开：`PartitionLog` 拥有一个分区，
`Segment` 拥有一对文件，`OffsetIndex` 缩小扫描起点。

## 学习目标

完成本章后，你应该能够：

1. 解释为何分区日志分为 closed segment 与 active segment；
2. 追踪 offset 分配、按大小滚动和稀疏索引查找；
3. 说明 batch CRC 保护什么；
4. 区分可修复的活跃尾部损坏与必须报错的历史损坏；
5. 本地复现滚动、索引重建和尾部截断。

## 从逻辑日志到 segment 文件

`src/minikafka/log/partition_log.py` 的 `PartitionLog.open` 扫描分区目录中
文件名 stem 为数字的 `*.log`。若不存在，就从 offset 0 创建 segment；
否则按 base offset 排序后逐个打开。除最后一个外都是 **closed**，最后一个
是 **active**。历史 closed segment 不应变化，只有 active tail 可能因追加
中断而留下不完整字节，这一区分决定恢复与维护策略。

`src/minikafka/log/segment.py` 的 `segment_stem` 把 base offset 2 格式化为：

```text
00000000000000000002.log
00000000000000000002.index
```

`.log` 保存完整编码 batch；`.index` 保存固定宽度的
`(relative offset, byte position)`。文件名 base 加相对 offset 即绝对
offset。固定宽度让校验与二分查找都很直接。

### 追加与 offset 所有权

客户端不分配存储 offset。`PartitionLog.append` 调用
`RecordBatch.assign(self.leo, self.leader_epoch)`，再进入
`PartitionLog.append_replica_batch`；后者要求 batch base offset 必须等于
当前 LEO。leader 负责分配，follower 追加已经分配的 batch 并校验连续性。

写入前，`append_replica_batch` 先编码以获得大小。若 active 已有数据，且
`active.size_bytes + encoded_size` 会超过
`MiniKafkaConfig.segment_max_bytes`，就调用 `PartitionLog._roll`：flush
旧 active，再以当前 LEO 创建新 segment。空 segment 会接纳一个超大 batch，
所以该配置是滚动阈值，不是 frame 的硬上限；否则超大 batch 会导致无穷滚动。

`Segment.append` 在文件尾写 frame，把 `Segment.leo` 推进到
`batch.next_offset`。第一个 batch 一定写索引；此后只有距上次索引位置达到
`index_interval_bytes` 才新增条目。因此索引是**稀疏**的：它只把扫描带到
答案附近，不为每条记录保存一行。

### 稀疏查找仍需扫描

`src/minikafka/log/index.py` 的 `OffsetIndex.floor_position` 使用
`bisect_right` 找到不大于目标 relative offset 的最大索引项。
`Segment.iter_batches` seek 到该字节位置，向前解码直到 batch 的
`next_offset` 穿过请求位置；`Segment.scan` 再展开记录并按精确 offset
过滤。

稀疏索引用小范围扫描换取小索引。日志才是事实来源：删除索引不能删除数据。
启动时 `Segment.open` 扫描日志，并根据有效 batch 重建索引。
`tests/log/test_recovery.py::test_missing_index_is_rebuilt_from_log` 把这一
契约变成了测试。

### Frame 与 CRC

`src/minikafka/core/batch_codec.py` 的 `encode_batch` 写出：

```text
magic ("MKB1") | payload length | CRC32(payload)
```

payload 包含格式版本、base offset、leader epoch、生产者身份与序列、事务
标志、offset delta 和记录。key、value、header value 使用有符号长度，因此
`None` 与空字节不同。`decode_batch` 在构造 `RecordBatch` 前校验 magic、
最大长度、完整 frame 长度、CRC、版本、flag、字段以及多余尾字节。

CRC 用来发现意外损坏，不是密码学认证。它只能说明存储字节与写入 frame
一致，不能证明写入者可信。

## 启动恢复：只修 active tail

`Segment.open` 从字节 0 连续读取 frame，同时校验 batch offset 不重叠、
不倒退。若解码失败：

- closed segment：关闭文件并抛 `StorageError`；
- active segment：截断到最后一个有效 batch 后的位置，然后继续打开。

这是对历史的 fail-closed 策略，也是对唯一合理中断位置的有界修复。若静默
丢弃中间损坏 segment，就会制造无法解释的洞。

扫描后，`Segment.open` 删除旧 index 并重建。`PartitionLog.open` 还扫描
已有 batch 以恢复最大 leader epoch。`src/minikafka/log/recovery.py` 的
`recover_active_segment` 只是薄封装；真正机制在 `Segment.open`。

`PartitionLog.truncate_to` 与启动修尾不同。复制层在已知 batch 边界上用它
丢弃分叉后缀。MiniKafka 收集保留 batch，删除全部 segment 文件，创建新
segment，再重新追加。第 6 章会把它连接到 promotion 与 leader epoch。

## MiniKafka 与 Apache Kafka 的对照

真实 Kafka 同样使用有序 segment、offset index、batch CRC，按配置大小或
时间滚动并恢复日志尾部。相关配置包括 `log.segment.bytes`、
`log.segment.ms` 和 `log.index.interval.bytes`。

表示方式则主动不同。MiniKafka 使用自定义、不压缩的 `MKB1` frame 和教学
索引格式，没有 Kafka record batch wire layout、压缩 codec、time index、
transaction index 或 memory-mapped index；它只按大小滚动。启动会扫描
每个 batch 恢复最大 leader epoch，Kafka 则维护 leader-epoch checkpoint。

MiniKafka 的 `PartitionLog.truncate_to` 把保留日志重写为一个新 segment；
Kafka 通常原位截断受影响的尾部文件与索引。详见[行为矩阵](../behavior-matrix.md)
和 [Kafka 映射](../kafka-mapping.md)。共享的不变量是“完整、有序 batch 和
明确有效边界”，而不是文件兼容。

## 动手实验：滚动并修复尾部

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run --offline python - <<'PY'
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.log.partition_log import PartitionLog

def one(value: bytes, timestamp: int) -> RecordBatch:
    return RecordBatch.unassigned((Record(None, value, timestamp),))

with TemporaryDirectory() as directory:
    root = Path(directory)
    config = MiniKafkaConfig(
        root, segment_max_bytes=120, index_interval_bytes=1
    )
    log = PartitionLog.open(root / "events-0", config)
    for i in range(3):
        log.append(one(f"value-{i}".encode(), i))
    print("segment bases:", [s.base_offset for s in log.segments])
    print("active index:", log.active.index.entries)
    active_path = log.active.log_path
    valid_size = log.active.size_bytes
    log.close()
    with active_path.open("ab") as file:
        file.write(b"MKB1\x00\x00\x00\x20partial")
    recovered = PartitionLog.open(root / "events-0", config)
    print("recovered leo:", recovered.leo)
    print("tail repaired:", recovered.active.size_bytes == valid_size)
    print("values:", [r.value for r in recovered.fetch(0, 10)])
    recovered.close()
PY
```

实测输出：

```text
segment bases: [0, 1, 2]
active index: ((2, 0),)
recovered leo: 3
tail repaired: True
values: [b'value-0', b'value-1', b'value-2']
```

很小的阈值使每个 batch 都进入新 segment。active 从 offset 2 开始，所以
第一项把 2 映射到字节 0。追加不完整 frame 模拟进程在写入中死亡。重开时
仅截掉无效 suffix，重建索引并保持 LEO 3。

不要把 `tail repaired: True` 推广为“所有损坏都会修复”。若翻转第一个
closed `.log` 中的字节，启动会抛 `StorageError`；拒绝启动保护了历史前缀。

## 练习

### 1. 理解题：为何接纳第一个超大 batch

解释 `PartitionLog.append_replica_batch` 中 `segment_max_bytes` 为何不是
严格 batch 上限。

验收：引用保护 `_roll` 的两个布尔条件，并说明接纳单个 batch 避免了什么
无法前进的情况。

??? note "参考答案"

    只有 `active.has_data` 为真，且新 frame 会越过阈值时才滚动。空 segment
    会接纳超大 batch；否则滚动后的新 segment 仍为空，相同决定会无限重复。

### 2. 动手题：证明索引可重建

扩展实验：关闭日志后删除 active `.index`，再重开并打印
`recovered.active.index.entries`。

验收：重开成功、值不变、index 文件重新出现，且 `git diff -- src` 无输出。

??? note "参考答案"

    重开前执行：

    ```python
    index_path = active_path.with_suffix(".index")
    index_path.unlink()
    ```

    `Segment.open` 扫描有效 frame 并调用 `OffsetIndex.create`。本配置下重建
    结果为 `((2, 0),)`。

### 3. 源码设计题：增加 time index

不改 `src/`，起草支持“首个 timestamp 不小于目标值”的稀疏时间索引方案，
至少指出三个改动位置和一个崩溃恢复测试。

验收：覆盖文件生命周期、追加、重建和 closed segment 错误策略；只增加
lookup 函数不算完成。

??? note "参考答案"

    需要定义 `TimeIndex` 文件格式；接入 `Segment` 的 create/open/flush/
    close；按 `batch.max_timestamp_ms` 追加条目；启动时与 offset index 一起
    重建；在 `PartitionLog` 的删除、压实交换与截断中更新文件生命周期；
    查询先用 segment 最大时间定位再扫描。恢复测试应删除 active time index，
    重开后验证 timestamp 查询不变；closed index 损坏必须有明确 fail-closed
    策略。

## 小结

分区日志是完整、CRC 保护的有序 batch 序列。segment 限定维护范围，稀疏
索引加速定位却不成为事实来源，启动恢复精确停在最后一个有效 active-tail
边界。第 3 章将在相同 segment 边界上删除旧前缀并压实过时 key，同时保留
有意义的 offset 空洞。
