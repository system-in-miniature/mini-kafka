# 第 3 章 · 保留与压实

只追加日志不能无限增长。Kafka 有两个互补的清理概念：retention 丢弃旧
segment 前缀，compaction 丢弃同一 key 的过时值。前者问“哪些旧历史可以
消失”，后者问“这个 key 的最新状态是哪条”。MiniKafka 把二者明确分开，
并让所有重写只作用于 closed segment。

## 学习目标

完成本章后，你应该能够：

1. 区分按时间/大小 retention 与 keyed compaction；
2. 解释 retention 为何只能删除连续的 closed-segment 前缀；
3. 推导 keyed、keyless 与 tombstone 记录的保留结果；
4. 说明重写后原 offset 与空洞为何保留；
5. 评估双目录交换的保证与崩溃窗口。

## Retention 删除前缀，而不是任意记录

`src/minikafka/log/retention.py` 的 `RetentionManager.apply` 接收
`PartitionLog` 以及可选时间、大小预算。负时间和非正 byte 预算会被拒绝。
管理器只考虑 `log.closed_segments`，active segment 永不入选。

时间 retention 计算：

```python
boundary = clock.now_ms() - retention_ms
```

它从旧到新遍历 closed segment，只要
`segment.max_timestamp_ms < boundary` 就选择；遇到第一个未过期 segment
立刻停止。停止很关键：即便更晚的 segment 恰好带有更老 timestamp，跳过去
删除也会制造物理中洞。Kafka 同样删除合格 segment 前缀，而不是从历史中
随意抽记录。

大小 retention 从时间已选择部分之后继续。它计算删除后剩余总字节，从最旧
closed segment 继续选择，直到满足 `retention_bytes` 或没有 closed segment。
组合时间与大小仍只得到一个前缀。

`src/minikafka/log/partition_log.py` 的
`PartitionLog.delete_closed_segments` 再次强制策略：拒绝 active 或未知
base offset，并把请求集合与最前面的 `len(requested)` 个 closed segment
比较，只有完全相同的连续前缀才允许。随后关闭句柄并删除 `.log/.index`。
新的 `log_start_offset` 是首个幸存 segment 的 base；旧 offset 不会重编号，
fetch 它们会抛 `OffsetOutOfRange`。

### 时间语义以 segment 为单位

只有 segment 的**最大**记录 timestamp 足够老，它才合格。一条新记录会让
整个 segment 保留。这种清理粒度较粗，却避免了仅为 retention 重写 segment。
因此 segment 大小和滚动频率会影响过期数据何时真正可删。

MiniKafka 在追加时维护 `Segment.max_timestamp_ms`，打开时重建；它没有独立
time index，所以能判断 segment 资格，却不支持 Kafka 的时间到 offset 查询。

## Compaction 保留每个 key 的最新值

`src/minikafka/log/compaction.py` 的 `LogCompactor.compact` 把 closed
segment 固定为 source。没有 closed segment 就返回空结果。否则
`_latest_offsets` 扫描**全部** segment（含 active），为每个非空 key 记录
最大 offset。

把 active 纳入 latest map 很微妙：若 closed 有 `a=old`，active 有
`a=new`，active 虽不被重写，旧值仍应消失。同时 active 的全部 batch 会
原样复制到替换目录。

`LogCompactor._compact_batch` 有三条规则：

1. `key is None` 的记录保留，因为没有 key 状态身份；
2. keyed record 只在绝对 offset 等于该 key 的 latest offset 时保留；
3. 最新 keyed tombstone（`value is None`）先保留，年龄大于
   `delete_retention_ms` 后才可消失。

Tombstone 不是空值，而是删除标记。保留一段时间让下游副本或消费者有机会
看到删除。过期后，旧值和标记都可不再出现在压实视图。

控制 batch 原样通过。若 batch 只剩部分记录，`dataclasses.replace` 保留
base offset 与 `last_offset_delta`，仅替换 `records/offset_deltas`。因此物理
记录可以只有 offset 1 和 3，而逻辑范围仍到 4。压实后的 offset 空洞是正常
现象；消费者不能假设“返回记录数等于 offset 距离”。

## 构建并安装压实目录

`PartitionLog.install_compacted_closed` 创建 sibling 临时目录，以当前
`log_start_offset` 创建 compacted segment，用 `allow_gap=True` 追加重写的
closed batch，再原样复制 active segment。新 segment 全部 flush、close 后
才发布。

发布使用两次 `os.replace`：

1. live 目录 → 唯一 backup；
2. temporary 目录 → live。

随后 fsync 父目录，重开 replacement，删除 backup。若第二次 rename 在进程
内抛异常，会把 backup 移回；若 `before_swap` 注入点抛异常，live 尚未移动，
temporary 会清理。

但两次 rename **不是一次原子交换**。进程或机器在两次之间崩溃，可能只剩
backup 而没有 live。代码注释明确承认这一点：它防止未构建完成的文件发布，
能处理进程内异常，却不是覆盖每个掉电点的完整恢复协议。

## MiniKafka 与 Apache Kafka 的对照

真实 Kafka 支持 `cleanup.policy=delete`、`compact` 或二者组合，重要配置有
`retention.ms`、`retention.bytes`、`segment.bytes`、
`delete.retention.ms`。log cleaner 在后台选择 dirty segment，使用更丰富的
文件生命周期与 cleaner checkpoint，还会处理事务标记、lag、cleanable
ratio 和多种索引。

MiniKafka 只在调用 `RetentionManager` 或 `LogCompactor` 时清理，没有后台
cleaner。compactor 把整个 closed 视图重建到 sibling 目录，并存在上述 rename
窗口；时间 retention 只有 segment maxima，没有 time index。可执行证据和
限制见[行为矩阵](../behavior-matrix.md)与
[Kafka 映射](../kafka-mapping.md)。

共享机制仍然重要：删除尊重 segment 边界，压实按 key 而不是按年龄，
tombstone 有保留窗口，记录消失后 offset 仍稳定。

## 动手实验：删前缀并压实 keyed state

```bash
UV_CACHE_DIR=/tmp/minikafka-uv-cache uv run --offline python - <<'PY'
from pathlib import Path
from tempfile import TemporaryDirectory
from minikafka.clock import ManualClock
from minikafka.config import MiniKafkaConfig
from minikafka.core.batch import RecordBatch
from minikafka.core.record import Record
from minikafka.log.compaction import LogCompactor
from minikafka.log.partition_log import PartitionLog
from minikafka.log.retention import RetentionManager

def append(log, *items):
    log.append(RecordBatch.unassigned(
        tuple(Record(k, v, ts) for k, v, ts in items)
    ))

with TemporaryDirectory() as directory:
    root = Path(directory)
    config = MiniKafkaConfig(root, index_interval_bytes=1)
    retained = PartitionLog.open(root / "retained", config)
    for ts in (0, 50, 150):
        append(retained, (None, str(ts).encode(), ts))
        retained.roll()
    append(retained, (None, b"250", 250))
    deleted = RetentionManager(ManualClock(300)).apply(
        retained, retention_ms=100, retention_bytes=None
    )
    print("retention deleted:", deleted, "start:", retained.log_start_offset)
    retained.close()

    compacted = PartitionLog.open(root / "compacted", config)
    append(compacted, (b"a", b"1", 0), (b"b", b"x", 1))
    compacted.roll()
    append(compacted, (b"a", b"2", 2), (None, b"event", 3))
    compacted.roll()
    append(compacted, (b"active", b"keep", 4))
    result = LogCompactor(
        ManualClock(10), delete_retention_ms=1000
    ).compact(compacted)
    print("compaction:", result.records_before, "->", result.records_after)
    print("records:", [
        (r.offset, r.key, r.value) for r in compacted.fetch(0, 10)
    ])
    compacted.close()
PY
```

实测输出：

```text
retention deleted: (0, 1, 2) start: 3
compaction: 4 -> 3
records: [(1, b'b', b'x'), (2, b'a', b'2'), (3, None, b'event'), (4, b'active', b'keep')]
```

时钟 300、窗口 100 ms 时，三个 closed segment 最大 timestamp 都小于 200，
因此 base 0、1、2 的前缀消失，base 3 的 active 保留。

压实检查四条 closed record：offset 0 的 `a=1` 过时；`b=x`、最新
`a=2` 和 keyless event 幸存。active record 被复制，但不计入 closed 的
`4 -> 3`。offset 0 的空洞证明地址保留而非重编号。

## 练习

### 1. 理解题：非单调 timestamp

三个 closed segment 最大 timestamp 为 250、0、0；时钟 300，
`retention_ms=100`。时间 retention 能删除哪些？

验收：严格按照 `RetentionManager.apply` 循环，并提到前缀不变量。

??? note "参考答案"

    一个也不能。边界为 200，最旧 segment 最大值 250，循环立即停止。不能
    跳过它删除后续项，否则会破坏连续前缀。

### 2. 动手题：观察 tombstone 过期

构建 closed segment：timestamp 0 的 `a=1` 和 timestamp 100 的
`a=None`。分别在新日志中用时钟 500、2000 以及
`delete_retention_ms=1000` 压实。

验收：第一次保留原 offset 的 tombstone；第二次值和 tombstone 都没有；
`git diff -- src` 无输出。

??? note "参考答案"

    时钟 500 时 tombstone 年龄 400，`_compact_batch` 保留最新 keyed record；
    时钟 2000 时年龄 1900，标记被丢弃。旧值在两次运行中都不是 latest，
    所以不会复活。

### 3. 源码设计题：恢复 rename 崩溃窗口

不应用代码，基于 live、backup、temporary 三类目录起草恢复协议。

验收：定义每种目录存在组合的启动决策，并指出何时必须先校验目录再选择。

??? note "参考答案"

    一种方案是在 rename 前写入并 fsync phase manifest。启动校验 live 与
    backup 中完整的 segment/index 对：live 有效则权威，旧 backup/temp 可
    隔离；live 缺失而 backup 有效则恢复 backup；只有 manifest 证明旧 live
    已备份且新文件均持久化时，才可提升有效 temp。状态歧义或两边都坏必须
    fail closed。选中的 live 与父目录持久化后才能删其余目录。

## 小结

Retention 与 compaction 都让记录消失，却保护不同契约。retention 删除合格
closed-segment 前缀并推进 log start；compaction 保留最新 keyed 状态、
keyless event 和未过期 tombstone，并留下 offset 空洞。MiniKafka 的 sibling
目录重写清楚展示了发布边界，也诚实暴露两次 rename 的崩溃窗口。第 4 章
回到写路径上游，研究生产者如何选分区、组 batch，并阻止 retry 重复追加。
