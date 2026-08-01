#!/usr/bin/env python3
"""Materialize the reviewed bilingual MiniKafka Stage lessons and layouts."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from journey.tools.extract_history import load_manifest

ROOT = Path(__file__).resolve().parents[2]
PATCH_HEADER = re.compile(r"^diff --git a/(.+?) b/(.+?)$", re.MULTILINE)
CHAPTER_SLUGS = {
    1: "01-getting-started",
    2: "02-the-log",
    3: "03-retention-compaction",
    4: "04-producer",
    5: "05-replication-basics",
    6: "06-isr-and-fencing",
    7: "07-consumer-groups",
    8: "08-transactions",
    9: "09-delivery-semantics",
    10: "10-protocol-and-beyond",
}


@dataclass(frozen=True, slots=True)
class LessonFacts:
    title_en: str
    title_zh: str
    problem_en: str
    problem_zh: str
    failure_en: str
    failure_zh: str
    concepts_en: str
    concepts_zh: str
    runtime_en: str
    runtime_zh: str
    statement_en: str
    statement_zh: str


FACTS = (
    LessonFacts(
        "Deterministic foundations",
        "确定性基础",
        "Storage and coordination logic cannot be reproduced if time, configuration, and failures have unstable meanings.",
        "如果时间、配置与失败没有稳定语义，存储和协调逻辑就无法复现。",
        "A clock that moves backwards or a configuration that accepts an impossible segment size lets later state machines start from invalid facts.",
        "允许时钟倒退或接受不可能 Segment 大小的配置，会让后续状态机从错误事实起步。",
        "A protocol clock is an injected source of time; validated configuration is an executable boundary; typed errors make failure classes observable without parsing messages.",
        "协议时钟是注入的时间来源；经过校验的配置是可执行边界；类型化错误让调用方无需解析文本即可识别失败类别。",
        "Callers read time through the Clock protocol, tests advance ManualClock explicitly, and configuration rejects invalid values before any log is opened.",
        "调用方只通过 Clock 协议读取时间，测试显式推进 ManualClock，配置在日志打开前拒绝非法值。",
        "The non-negative advance guard preserves monotonic time, while stable error codes preserve machine-readable failure identity.",
        "非负推进检查保持时间单调；稳定错误码保持机器可读的失败身份。",
    ),
    LessonFacts(
        "Binary-safe record batches",
        "二进制安全 Record Batch",
        "A broker needs one durable frame that preserves arbitrary bytes and can detect corruption before records enter the log.",
        "Broker 需要一种既能保存任意字节、又能在 Record 进入日志前发现损坏的持久帧。",
        "Round-trip tests include binary keys, values, and headers; corruption and truncation tests prove a decoder must reject plausible-looking partial data.",
        "Round-trip 测试包含二进制 Key、Value 与 Header；损坏和截断测试证明 Decoder 必须拒绝看似合理的残缺数据。",
        "A Record is user data; a RecordBatch is the append and replication unit; framing states lengths explicitly; CRC authenticates the encoded payload rather than Python objects.",
        "Record 是用户数据；RecordBatch 是追加与复制单元；Framing 显式记录长度；CRC 校验编码后的 Payload 而非 Python 对象。",
        "Encoding assigns a stable binary layout and checksum; decoding validates size, version, control/data invariants, and CRC before constructing domain objects.",
        "编码建立稳定二进制布局和校验和；解码在构造领域对象前验证长度、版本、控制/数据约束与 CRC。",
        "Length checks establish safe read boundaries first; only then may the CRC and semantic fields be trusted.",
        "长度检查先建立安全读取边界；之后 CRC 与语义字段才值得信任。",
    ),
    LessonFacts(
        "Sparse offset lookup",
        "稀疏 Offset 查找",
        "Scanning every batch from the start of a segment turns random offset reads into linear work.",
        "每次按 Offset 读取都从 Segment 开头扫描，会把随机读取退化为线性工作。",
        "Floor-lookup tests ask for offsets between entries and malformed-entry tests expose indexes that silently return a position after the requested record.",
        "Floor Lookup 测试查询两个条目之间的 Offset；损坏条目测试暴露会悄悄返回目标之后位置的索引。",
        "A sparse index maps selected relative offsets to byte positions. Floor lookup returns the closest indexed position not greater than the target, after which the log scans forward.",
        "稀疏索引把部分相对 Offset 映射到字节位置。Floor Lookup 返回不大于目标的最近位置，日志再从那里向前扫描。",
        "Append enforces monotonic entries, lookup performs ordered floor search, and truncation keeps index state aligned with a shortened log.",
        "Append 保证条目单调，Lookup 执行有序 Floor Search，Truncate 让索引与缩短后的日志保持一致。",
        "Returning a floor rather than an exact match makes sparsity correct: the index narrows the scan without claiming to locate every record.",
        "返回 Floor 而非强求精确命中，让稀疏性仍然正确：索引只缩小扫描范围，不声称定位每条 Record。",
    ),
    LessonFacts(
        "Recoverable log segments",
        "可恢复日志段",
        "A process can stop between writing a frame and updating its index, leaving disk state that is neither clean nor safely ignorable.",
        "进程可能在写 Frame 与更新索引之间停止，留下既不完整又不能安全忽略的磁盘状态。",
        "Recovery tests append truncated bytes and corrupt frames, then distinguish a removable incomplete tail from corruption inside the durable prefix.",
        "恢复测试追加截断字节与损坏 Frame，并区分可移除的不完整尾部和持久前缀内部的损坏。",
        "A Segment owns a log file and sparse index. Recovery scans complete batches, rebuilds derived index state, and truncates only an incomplete final frame.",
        "Segment 拥有日志文件与稀疏索引。恢复过程扫描完整 Batch、重建派生索引，并且只截断最后一个不完整 Frame。",
        "Append writes encoded batches and periodically indexes offsets; reopen validates the durable prefix and reconstructs next-offset and byte-position state.",
        "Append 写入编码 Batch 并周期性建立索引；重开时验证持久前缀并重建 Next Offset 与 Byte Position。",
        "Tail truncation is safe only when the decoder proves the failure begins at the final incomplete frame; mid-log corruption must remain visible.",
        "只有 Decoder 证明失败始于最后一个不完整 Frame 时，尾部截断才安全；日志中段损坏必须暴露出来。",
    ),
    LessonFacts(
        "Segmented partition log",
        "分段 Partition Log",
        "One ever-growing file makes retention, recovery, and bounded lookup difficult to reason about or operate.",
        "单个无限增长文件会让 Retention、Recovery 与有界查找难以推理和运维。",
        "Rolling and restart tests cross segment boundaries, then prove offsets and reads remain continuous after the process reopens the directory.",
        "滚动与重启测试跨越 Segment 边界，并证明进程重开目录后 Offset 与读取仍连续。",
        "A PartitionLog is an ordered sequence of immutable closed segments plus one active segment. Base offsets define the global address space.",
        "PartitionLog 是若干不可变 Closed Segment 加一个 Active Segment 的有序序列。Base Offset 定义全局地址空间。",
        "Append rolls before a size limit would be exceeded; reads choose a candidate segment and scan batches; reopen orders files and recovers the active tail.",
        "Append 在超过大小限制前 Roll；Read 选择候选 Segment 再扫描 Batch；重开时排序文件并恢复 Active Tail。",
        "Rolling before append keeps every completed segment within its configured bound while preserving one monotonic partition offset sequence.",
        "在 Append 前滚动，使每个完成 Segment 保持在配置边界内，同时维持单调的 Partition Offset 序列。",
    ),
    LessonFacts(
        "Topics and direct administration",
        "Topic 与直接管理接口",
        "Durable logs are not yet a broker: callers need named topics, partition metadata, replica placement, and one semantic API.",
        "持久日志还不是 Broker：调用方需要具名 Topic、Partition Metadata、Replica Placement 与统一语义 API。",
        "Administration tests reject invalid names and replication factors, then reopen the cluster to prove metadata and log directories agree.",
        "管理测试拒绝非法名称与复制因子，并重开 Cluster 证明 Metadata 与日志目录一致。",
        "TopicPartition is the stable address of one ordered log. Metadata records leader and replicas. The Direct API is the semantic reference rather than a transport protocol.",
        "TopicPartition 是单条有序日志的稳定地址。Metadata 记录 Leader 与 Replica。Direct API 是语义参考而非传输协议。",
        "Create-topic validates the request, assigns replicas deterministically, creates partition logs, persists metadata, and exposes the result through DirectAdmin.",
        "Create Topic 校验请求、确定性分配 Replica、创建 Partition Log、持久化 Metadata，并通过 DirectAdmin 暴露结果。",
        "Metadata must be persisted with the same partition identity used to locate logs; otherwise restart reconstructs a different broker topology.",
        "Metadata 必须用与日志定位相同的 Partition 身份持久化，否则重启会重建出不同拓扑。",
    ),
    LessonFacts(
        "Partitioned producer batching",
        "分区化 Producer Batching",
        "Per-record appends waste batching opportunities, while unbounded buffering or unstable partition choice breaks latency and ordering expectations.",
        "逐 Record 追加浪费批处理机会；无界缓冲或不稳定分区选择又会破坏延迟与顺序预期。",
        "Tests force both linger and batch-size flush paths, fill the bounded buffer, and mix keyed, keyless, and explicit partition sends.",
        "测试分别触发 Linger 与 Batch Size Flush，填满有界缓冲，并混合 Keyed、Keyless 与显式分区发送。",
        "The Partitioner chooses a partition; the Accumulator owns pending per-partition batches; linger is a latency bound; batch size is a throughput trigger.",
        "Partitioner 选择分区；Accumulator 拥有各分区待发送 Batch；Linger 是延迟边界；Batch Size 是吞吐触发器。",
        "Send chooses one partition, enqueues a pending record, and schedules flush when size or time closes the batch; acknowledgements resolve record futures in partition order.",
        "Send 选定一个 Partition、排入 Pending Record，并在大小或时间关闭 Batch 时 Flush；Ack 按分区顺序完成 Record Future。",
        "A keyed record hashes stably and an open keyless batch stays sticky, so batching efficiency never silently changes same-partition order.",
        "Keyed Record 稳定哈希，打开的 Keyless Batch 保持 Sticky，因此批处理效率不会悄悄改变同分区顺序。",
    ),
    LessonFacts(
        "Consumer position and replay",
        "Consumer Position 与重放",
        "Reading a record and declaring it processed are different events; collapsing them makes crash behavior impossible to control.",
        "读取 Record 与声明处理完成是两个事件；把它们合并会让崩溃行为无法控制。",
        "Delivery tests crash once before commit and once after commit, making replay and skip behavior visible instead of describing it abstractly.",
        "投递测试分别在 Commit 前后模拟崩溃，让 Replay 与 Skip 行为直接可见。",
        "Position is the next offset in one consumer instance. A committed offset is durable group progress. Earliest/latest reset policy supplies a starting point only when no commit exists.",
        "Position 是单个 Consumer 实例的下一 Offset；Committed Offset 是持久 Group 进度；只有不存在 Commit 时，Earliest/Latest 才提供起点。",
        "Poll advances local positions as records are returned; commit persists selected next offsets; a reopened consumer initializes from committed state or reset policy.",
        "Poll 返回记录时推进本地 Position；Commit 持久化选定 Next Offset；重开 Consumer 从 Commit 或 Reset Policy 初始化。",
        "Advancing position during poll is safe because durable progress changes only at commit; this separation defines at-least-once and at-most-once failure windows.",
        "Poll 时推进 Position 是安全的，因为持久进度只在 Commit 时变化；这一区分定义了 At-least-once 与 At-most-once 的失败窗口。",
    ),
    LessonFacts(
        "Consumer-group ownership",
        "Consumer Group 所有权",
        "Independent consumers can read the same partition concurrently unless the group has one authoritative assignment and generation.",
        "如果 Group 没有权威 Assignment 与 Generation，多个 Consumer 会并发读取同一 Partition。",
        "Rebalance tests join, leave, expire heartbeats, and then attempt stale-generation and non-owner commits.",
        "Rebalance 测试执行 Join、Leave、Heartbeat Expiry，并尝试旧 Generation 与非 Owner Commit。",
        "A group generation versions an assignment. Heartbeats extend membership leases. The assignor gives each subscribed partition one owner. Fencing rejects actions based on old ownership.",
        "Group Generation 为 Assignment 建版本；Heartbeat 延长成员 Lease；Assignor 让每个订阅 Partition 只有一个 Owner；Fencing 拒绝旧所有权操作。",
        "Join or membership expiry recomputes assignments and increments generation; consumers refresh their view; commit checks member, generation, and partition ownership together.",
        "Join 或成员过期会重算 Assignment 并增加 Generation；Consumer 刷新视图；Commit 同时检查 Member、Generation 与 Partition Ownership。",
        "The generation check must be atomic with ownership validation, or a consumer can commit after the assignment it observed has already disappeared.",
        "Generation 检查必须与 Ownership 校验原子完成，否则 Consumer 会在其观察到的 Assignment 已失效后仍然 Commit。",
    ),
    LessonFacts(
        "Prefix-only retention",
        "仅删除前缀的 Retention",
        "Disk limits require deletion, but removing arbitrary bytes or the active segment would break offset continuity and recovery.",
        "磁盘限制要求删除数据，但任意删除字节或 Active Segment 会破坏 Offset 连续性与恢复。",
        "Time and size tests create several segments and prove deletion removes only eligible closed prefixes while preserving the active tail.",
        "时间与大小测试创建多个 Segment，并证明删除只作用于符合条件的 Closed Prefix，Active Tail 始终保留。",
        "Retention is physical deletion of old closed segments. Log start offset advances, while existing record offsets never change. The active segment is not a deletion candidate.",
        "Retention 是对旧 Closed Segment 的物理删除。Log Start Offset 前进，但已有 Record Offset 永不重编号。Active Segment 不是删除候选。",
        "The manager evaluates age or total bytes from oldest to newest, stops at the first ineligible boundary, deletes selected files, and refreshes the partition view.",
        "Manager 从旧到新评估 Age 或总字节，在第一个不符合边界处停止，删除选中的文件并刷新 Partition 视图。",
        "Deleting a prefix preserves a single monotonic log-start boundary; deleting a middle segment would create an unexplained physical hole.",
        "删除前缀能保持唯一单调的 Log Start Boundary；删除中间 Segment 会制造无法解释的物理空洞。",
    ),
    LessonFacts(
        "Keyed log compaction",
        "按 Key 的日志压实",
        "Retention controls age and size but cannot preserve the latest state per key while discarding superseded values.",
        "Retention 控制年龄与大小，却无法在丢弃旧值时保留每个 Key 的最新状态。",
        "Compaction tests include duplicate keys, tombstones, offset gaps, and an injected swap failure so a partially replaced segment set cannot become authoritative.",
        "Compaction 测试包含重复 Key、Tombstone、Offset Gap 与注入的 Swap Failure，避免部分替换的 Segment 集合成为权威状态。",
        "Compaction retains the latest record for each key in a chosen closed range. Tombstones represent deletion. Logical offsets remain unchanged even when physical records disappear.",
        "Compaction 在选定 Closed Range 中保留每个 Key 的最新 Record；Tombstone 表示删除；即使物理记录消失，逻辑 Offset 也不变。",
        "The compactor scans newest knowledge, rewrites retained records to temporary segments, fsyncs them, atomically swaps the range, and only then removes old files.",
        "Compactor 扫描最新 Key 状态，把保留记录写入临时 Segment、Fsync、原子交换范围，之后才删除旧文件。",
        "Preserving original offsets makes compaction a storage rewrite rather than a new log; the atomic swap makes either old or new segments recoverable.",
        "保留原 Offset 让 Compaction 只是存储重写而非新日志；原子 Swap 确保恢复时看到完整旧版本或新版本。",
    ),
    LessonFacts(
        "ISR and high watermark",
        "ISR 与 High Watermark",
        "Leader-local append is not committed data when followers may lag or fail.",
        "当 Follower 可能延迟或失败时，Leader 本地 Append 不等于已提交数据。",
        "Follower-fetch tests move complete batches, ISR tests expire lagging replicas, and visibility tests refuse reads beyond the high watermark.",
        "Follower Fetch 测试移动完整 Batch，ISR 测试让落后 Replica 过期，可见性测试拒绝读取 High Watermark 之后的数据。",
        "LEO is a replica's next offset. ISR is the current caught-up replica set. High watermark is the minimum LEO across ISR and bounds committed visibility.",
        "LEO 是 Replica 的 Next Offset；ISR 是当前追上的 Replica 集合；High Watermark 是 ISR 中最小 LEO，限制已提交可见性。",
        "Followers fetch from their LEO, append complete batches, report progress, and cause membership and HW to refresh; committed reads stop at HW.",
        "Follower 从自身 LEO Fetch、追加完整 Batch、报告进度，并触发 Membership 与 HW 刷新；Committed Read 停在 HW。",
        "Taking the minimum ISR LEO proves every current in-sync replica contains the visible prefix; using the leader LEO would expose unreplicated data.",
        "取 ISR 最小 LEO 证明每个当前同步副本都拥有可见前缀；使用 Leader LEO 会暴露未复制数据。",
    ),
    LessonFacts(
        "Acknowledgement modes",
        "确认模式",
        "One word such as successful is ambiguous unless the producer knows which durability boundary acknowledged the write.",
        "如果 Producer 不知道是哪条持久性边界确认了写入，一个“成功”没有明确含义。",
        "The contract contrasts `acks=0`, `acks=1`, and `acks=all`, then shrinks ISR both before and after append to expose two distinct failure windows.",
        "契约对比 `acks=0`、`acks=1` 与 `acks=all`，并分别在 Append 前后收缩 ISR，暴露两个不同失败窗口。",
        "`acks=0` does not return offsets, `acks=1` waits for leader append, and `acks=all` requires the acknowledgement set plus `min.insync.replicas`.",
        "`acks=0` 不返回 Offset；`acks=1` 等待 Leader Append；`acks=all` 要求 Ack Set 并满足 `min.insync.replicas`。",
        "The replica set checks admission before append, tracks required acknowledgers, advances replicas, and fails the waiter if ISR loses a required member before completion.",
        "Replica Set 在 Append 前检查准入、记录 Required Acknowledger、推进 Replica，并在完成前 ISR 丢失必需成员时让 Waiter 失败。",
        "Pre-append rejection prevents an under-replicated write; post-append failure prevents a leader-local tail from being mislabeled as fully acknowledged.",
        "Append 前拒绝避免写入进入低副本状态；Append 后失败避免把仅在 Leader Tail 的数据误标为完整确认。",
    ),
    LessonFacts(
        "Promotion and epoch fencing",
        "晋升与 Epoch Fencing",
        "After failover, an old leader may still accept traffic and carry a suffix the new leader never committed.",
        "Failover 后旧 Leader 可能仍接收流量，并携带新 Leader 从未提交的后缀。",
        "Promotion tests reject non-ISR candidates, increment epochs, issue stale requests, and force the old leader to truncate an uncommitted tail.",
        "Promotion 测试拒绝非 ISR Candidate、增加 Epoch、发送过期请求，并迫使旧 Leader 截断未提交 Tail。",
        "Leader epoch versions authority. Clean promotion chooses an eligible ISR replica. Divergent data after HW is uncommitted and must not survive reconciliation.",
        "Leader Epoch 为权威建版本；Clean Promotion 选择合格 ISR Replica；HW 之后的 Divergent Data 未提交，协调时不得保留。",
        "Promotion validates eligibility, increments and persists the epoch, changes metadata, truncates replicas to the committed boundary, and fences requests carrying older epochs.",
        "Promotion 校验资格、增加并持久化 Epoch、修改 Metadata、把 Replica 截到 Commit Boundary，并 Fencing 携带旧 Epoch 的请求。",
        "Comparing the request epoch before mutation turns stale authority into a typed failure; truncating to HW preserves only the agreed prefix.",
        "在修改前比较 Request Epoch，把过期权威变成类型化失败；截断到 HW 只保留已达成共识的前缀。",
    ),
    LessonFacts(
        "Idempotent producer retries",
        "幂等 Producer 重试",
        "A lost response makes retry necessary, but a blind retry duplicates a record that may already be durable.",
        "响应丢失会迫使重试，但盲目重试会复制可能已经持久化的 Record。",
        "Tests resend the exact sequence, create a sequence gap, restart producer state, and start a second instance with the same transactional identity.",
        "测试重发相同 Sequence、制造 Sequence Gap、重启 Producer State，并用同一身份启动第二实例。",
        "Producer ID and epoch identify authority; per-partition sequence numbers identify order. An exact retry returns the original offsets, while gaps and old epochs are fenced.",
        "Producer ID 与 Epoch 标识权威；每 Partition Sequence 标识顺序。精确重试返回原 Offset，Gap 与旧 Epoch 被 Fencing。",
        "Before append, the state manager compares epoch and sequence with durable per-partition state; after append, it records the batch result for replay across restart.",
        "Append 前 State Manager 用持久的分区状态比较 Epoch 与 Sequence；Append 后记录 Batch Result，使重启后仍可复用。",
        "Only the next sequence may append, while the immediately repeated sequence may reuse its stored result; these are different branches, not one loose inequality.",
        "只有 Next Sequence 可以追加，而刚完成的重复 Sequence 可以复用保存结果；这是两个分支，不是一个宽松不等式。",
    ),
    LessonFacts(
        "Transactional records and offsets",
        "事务化 Record 与 Offset",
        "Publishing output and committing input progress separately can expose partial work after a crash.",
        "分别发布输出与提交输入进度，会在崩溃后暴露部分完成的工作。",
        "Visibility tests pause between prepare and commit, abort tests hide data, offset tests publish output and input progress together, and journal tests cut the durable tail.",
        "可见性测试停在 Prepare 与 Commit 之间；Abort 测试隐藏数据；Offset 测试让输出与输入进度一起发布；Journal 测试截断持久尾部。",
        "A transaction has an epoch and state. Control batches mark commit or abort. `read_committed` filters unresolved and aborted data. The journal drives recovery of prepared decisions.",
        "Transaction 具有 Epoch 与 State；Control Batch 标记 Commit 或 Abort；`read_committed` 过滤未决与已中止数据；Journal 驱动 Prepared Decision 恢复。",
        "Begin fences old epochs, send appends transactional batches, prepare durably records intent, commit writes markers and offsets, and recovery completes or aborts incomplete work deterministically.",
        "Begin Fencing 旧 Epoch；Send 追加事务 Batch；Prepare 持久记录意图；Commit 写 Marker 与 Offset；Recovery 确定性完成或中止未完成工作。",
        "Output visibility and input-offset publication must share the commit decision; otherwise recovery can duplicate output or skip input.",
        "输出可见性与输入 Offset 发布必须共享同一 Commit Decision，否则恢复会重复输出或跳过输入。",
    ),
    LessonFacts(
        "Thin JSON TCP adapter",
        "轻量 JSON TCP Adapter",
        "A network API is useful only if framing and translation do not create a second, divergent broker semantics.",
        "网络 API 只有在 Framing 与 Translation 不制造第二套分叉 Broker 语义时才有价值。",
        "Parity tests execute equivalent Direct and TCP operations; framing tests split stream input, carry binary values, and require typed domain errors in JSON responses.",
        "Parity 测试执行等价 Direct 与 TCP 操作；Framing 测试拆分 Stream 输入、携带二进制值，并要求 JSON Response 返回类型化领域错误。",
        "The adapter uses a length-prefixed JSON envelope. Binary fields require explicit encoding. Dispatch translates requests to the Direct semantic core and maps domain failures back to stable wire errors.",
        "Adapter 使用长度前缀 JSON Envelope；二进制字段需要显式编码；Dispatch 把请求翻译到 Direct 语义核心，并把领域失败映射回稳定 Wire Error。",
        "The server reads an exact frame length, validates request shape, calls existing cluster operations, and serializes results without owning storage or replication decisions.",
        "Server 精确读取 Frame 长度、校验 Request Shape、调用已有 Cluster Operation，再序列化结果；它不拥有存储或复制决策。",
        "A transport handler may translate types but must not reimplement acknowledgement, visibility, or offset rules; parity evidence protects that boundary.",
        "Transport Handler 可以翻译类型，但不得重写 Ack、Visibility 或 Offset 规则；Parity Evidence 保护这条边界。",
    ),
    LessonFacts(
        "Lifecycle and failure labs",
        "生命周期与失败实验",
        "Background tasks can fail after an API call returns, and an unowned shutdown can leave tasks, sockets, or buffered records behind.",
        "后台任务可能在 API 返回后失败；没有统一所有权的 Shutdown 会遗留 Task、Socket 或 Buffered Record。",
        "Failure tests inject a background exception and require the next boundary to surface it; shutdown tests verify flush, task cancellation, and idempotent close.",
        "Failure 测试注入后台异常并要求下一个边界暴露它；Shutdown 测试验证 Flush、Task Cancellation 与幂等 Close。",
        "Lifecycle ownership means one component starts, observes, and closes every resource it creates. A failure injector makes asynchronous faults deterministic. Labs are runnable evidence, not alternate implementations.",
        "生命周期所有权意味着组件负责启动、观测并关闭自己创建的每个资源；Failure Injector 让异步故障确定化；Lab 是可运行证据而非另一套实现。",
        "Cluster and producer close paths stop new work, flush pending records, await or cancel owned tasks, surface captured failures, and tolerate repeated close calls.",
        "Cluster 与 Producer 的 Close Path 停止新工作、Flush Pending Record、等待或取消自有 Task、暴露捕获失败，并允许重复 Close。",
        "A background exception must cross an owned API boundary; logging it without changing observable state would turn data loss into apparent success.",
        "后台异常必须穿过一个有所有权的 API Boundary；只记录日志却不改变可观察状态，会把数据丢失伪装成成功。",
    ),
    LessonFacts(
        "ISR rejoin regression",
        "ISR Rejoin 回归",
        "A follower can be close to the leader LEO yet still sit behind the committed high watermark, so LEO-only rejoin admits an unsafe election candidate.",
        "Follower 可能接近 Leader LEO 却仍落后于已提交 High Watermark，因此只看 LEO 的 Rejoin 会接纳不安全的选举候选。",
        "The regression constructs a replica behind HW, asks it to rejoin, and then attempts promotion; the old predicate would let both operations succeed.",
        "回归测试构造落后 HW 的 Replica，尝试让它 Rejoin，再尝试 Promotion；旧谓词会让两步都成功。",
        "ISR membership is a safety claim, not a freshness hint. A rejoining replica must contain the entire committed prefix before it can acknowledge or become leader.",
        "ISR Membership 是安全承诺而非新鲜度提示。Rejoin Replica 必须拥有完整 Commit Prefix，才能参与 Ack 或成为 Leader。",
        "Membership refresh compares follower LEO with both lag policy and HW; only replicas at or beyond HW reenter the set used by acknowledgement and election.",
        "Membership Refresh 同时比较 Follower LEO、Lag Policy 与 HW；只有到达 HW 的 Replica 才能重新进入 Ack 与 Election 集合。",
        "The `leo >= high_watermark` conjunct is the missing safety gate: without it, shrinking leader progress can make an incomplete replica appear caught up.",
        "`leo >= high_watermark` 这个合取条件是缺失的安全门：没有它，Leader Progress 变化会让不完整 Replica 看似已追上。",
    ),
    LessonFacts(
        "Cross-mechanism domain closure",
        "跨机制领域闭环",
        "Individually correct features can still violate one another when transactions, retention, replication visibility, restart, and the public API meet.",
        "各自正确的功能在事务、Retention、复制可见性、重启与公共 API 交汇时仍可能互相破坏。",
        "The final contracts combine rebalance, failover, restart, prefix retention, abort visibility, and prepared recovery; each test exposes a bug that isolated happy paths miss.",
        "最终契约组合 Rebalance、Failover、Restart、Prefix Retention、Abort Visibility 与 Prepared Recovery；每条测试都暴露孤立 Happy Path 看不到的问题。",
        "Domain closure means shared invariants survive composition. Transactional writes require `acks=all`; retention deletes only prefixes; `read_committed` uses replica-level transaction knowledge; public exports name the supported semantic surface.",
        "领域闭环意味着共享不变量在组合后仍成立。事务写要求 `acks=all`；Retention 只删前缀；`read_committed` 使用 Replica 级事务知识；公共导出明确支持的语义表面。",
        "The cluster routes transactional appends through replicated acknowledgement, replica reads consult commit markers, retention advances one log-start boundary, and restart restores metadata, offsets, producer, and transaction state.",
        "Cluster 让事务 Append 经过复制确认；Replica Read 查询 Commit Marker；Retention 推进唯一 Log Start Boundary；Restart 恢复 Metadata、Offset、Producer 与 Transaction State。",
        "The final integration checks are not new features: they prove that local invariants use the same authority and durability boundaries when composed.",
        "最终集成检查不是新功能；它证明局部不变量组合时仍使用同一套权威与持久性边界。",
    ),
)


def quoted(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def is_supporting(path: str) -> bool:
    return path in {"pyproject.toml", "uv.lock"} or path.endswith("/__init__.py")


def markers(paths: list[str]) -> str:
    return "\n".join(f"<!-- journey-file: {path} -->" for path in paths)


def deliverables(paths: list[str]) -> str:
    return "\n".join(f"- `{path}`" for path in paths)


def representative_assertion(patch: str) -> str:
    for line in patch.splitlines():
        if re.match(r"^\+\s*assert\s+", line):
            return line[1:].strip()
    raise ValueError("each Stage must add or modify a visible assertion")


def lesson_body(
    *,
    facts: LessonFacts,
    paths: list[str],
    tests: list[str],
    mechanisms: list[str],
    supporting: list[str],
    chapter: int,
    chinese: bool,
    stage_number: int,
    test_statement: str,
) -> str:
    if chinese:
        goal = (
            f"实现{facts.title_zh}，并能从可执行失败、运行时状态与关键语句解释其边界。"
        )
        test_walk = f"""{markers(tests)}
#### {facts.title_zh}测试证据

##### 测试锁定什么

这些测试锁定本 Stage 的正常路径、边界条件、失败可见性与恢复不变量。

##### 如何构造反例

{facts.failure_zh}

##### 关键测试语句

```python
{test_statement}
```

这条断言把外部可观察结果与内部持久性或权威边界绑定起来，而不是只检查调用是否返回。

##### 失败意味着什么

失败说明实现跨越了本 Stage 刚建立的安全、顺序、可见性或恢复边界。"""
        mechanism_walk = f"""{markers(mechanisms)}
#### {facts.title_zh}机制

##### 是什么，为什么现在需要

{facts.concepts_zh}

##### 在运行时做什么

{facts.runtime_zh}

##### 关键语句理解

{facts.statement_zh}"""
        support_walk = (
            f"""{markers(supporting)}
#### 包与工程支撑

这些文件只负责让本 Stage 的包边界、依赖与测试环境可复现，不把脚手架误讲成 Kafka 机制。"""
            if supporting
            else ""
        )
        return f"""### 目标

{goal}

### 交付文件

{deliverables(paths)}

### 当前遇到的问题

{facts.problem_zh}

### 测试契约

#### 先看会坏在哪里

{facts.failure_zh}

{test_walk}

### 基本概念

{facts.concepts_zh}

### 为什么需要这个机制

{facts.problem_zh} 如果不建立明确契约，后续机制只能建立在偶然行为上。

### 运行时心智模型

{facts.runtime_zh}

### 机制板块

{mechanism_walk}

{support_walk}

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/{stage_number:02d}-{SLUGS[stage_number - 1]}/tests.txt)`，再用 Journey Check 比较累计源码与标准 Stage。

### 需要真正记住的内容

{facts.statement_zh}

### 用自己的话讲清楚

请解释这个 Stage 解决的失败窗口、运行时状态如何变化，以及哪条语句守住边界。

### 教材

[第 {chapter} 章](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/zh/tutorial/{CHAPTER_SLUGS[chapter]}.md)"""

    goal = f"Build {facts.title_en.lower()} and explain its boundary from executable failure, runtime state, and the critical statement."
    test_walk = f"""{markers(tests)}
#### {facts.title_en} test evidence

##### What this test locks

These tests lock the Stage's happy path, boundary conditions, visible failures, and recovery invariants.

##### How it constructs the counterexample

{facts.failure_en}

##### Key test statement

```python
{test_statement}
```

This assertion binds the externally visible result to the internal durability or authority boundary, rather than merely checking that a call returned.

##### What a failure means

A failure means the implementation crossed the safety, ordering, visibility, or recovery boundary introduced by this Stage."""
    mechanism_walk = f"""{markers(mechanisms)}
#### {facts.title_en} mechanism

##### What it is and why it appears

{facts.concepts_en}

##### Runtime role

{facts.runtime_en}

##### Statement understanding

{facts.statement_en}"""
    support_walk = (
        f"""{markers(supporting)}
#### Package and project support

These files only keep the package boundary, dependencies, and test environment reproducible; they are supporting wiring rather than Kafka mechanism logic."""
        if supporting
        else ""
    )
    return f"""### Goal

{goal}

### Deliverable files

{deliverables(paths)}

### The problem at this point

{facts.problem_en}

### Test contract

#### See the failure first

{facts.failure_en}

{test_walk}

### Basic concepts

{facts.concepts_en}

### Why this mechanism is necessary

{facts.problem_en} Without an explicit contract, every later mechanism would depend on accidental behavior.

### Runtime mental model

{facts.runtime_en}

### Mechanism blocks

{mechanism_walk}

{support_walk}

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/{stage_number:02d}-{SLUGS[stage_number - 1]}/tests.txt)`, then use Journey Check to compare the cumulative source with the canonical Stage.

### Durable takeaways

{facts.statement_en}

### Explain it in your own words

Explain the failure window this Stage closes, how runtime state changes, and which statement protects the boundary.

### Textbook

[Chapter {chapter}](https://github.com/system-in-miniature/mini-kafka/blob/main/docs/tutorial/{CHAPTER_SLUGS[chapter]}.md)"""


def layout_text(
    tests: list[str], mechanisms: list[str], supporting: list[str], facts: LessonFacts
) -> str:
    lines = [f"failure_files = [{', '.join(quoted(path) for path in tests)}]", ""]
    if mechanisms:
        lines.extend(
            [
                "[[blocks]]",
                'id = "mechanism"',
                f"title_en = {quoted(facts.title_en + ' mechanism')}",
                f"title_zh = {quoted(facts.title_zh + '机制')}",
                f"summary_en = {quoted(facts.runtime_en)}",
                f"summary_zh = {quoted(facts.runtime_zh)}",
                f"files = [{', '.join(quoted(path) for path in mechanisms)}]",
                "",
            ]
        )
    if supporting:
        lines.extend(
            [
                "[[blocks]]",
                'id = "supporting"',
                'title_en = "Package and project support"',
                'title_zh = "包与工程支撑"',
                'summary_en = "Keep package exports, dependencies, and the test environment reproducible."',
                'summary_zh = "保持包导出、依赖与测试环境可复现。"',
                f"files = [{', '.join(quoted(path) for path in supporting)}]",
                "supporting = true",
                "",
            ]
        )
    return "\n".join(lines)


def main() -> int:
    manifest = load_manifest(ROOT / "journey" / "manifest.toml")
    if len(manifest.stages) != len(FACTS):
        raise ValueError("lesson facts must match the twenty-Stage manifest")
    global SLUGS
    SLUGS = [stage.slug for stage in manifest.stages]
    for stage, facts in zip(manifest.stages, FACTS, strict=True):
        directory = ROOT / "journey" / "stages" / f"{stage.number:02d}-{stage.slug}"
        patch = (directory / "stage.patch").read_text()
        paths = [match.group(2) for match in PATCH_HEADER.finditer(patch)]
        tests = [path for path in paths if path.startswith("tests/")]
        production = [path for path in paths if not path.startswith("tests/")]
        supporting = [path for path in production if is_supporting(path)]
        mechanisms = [path for path in production if path not in supporting]
        if not tests or not mechanisms:
            raise ValueError(f"Stage {stage.number:02d} needs test and mechanism diffs")
        english = lesson_body(
            facts=facts,
            paths=paths,
            tests=tests,
            mechanisms=mechanisms,
            supporting=supporting,
            chapter=stage.chapter,
            chinese=False,
            stage_number=stage.number,
            test_statement=representative_assertion(patch),
        )
        chinese = lesson_body(
            facts=facts,
            paths=paths,
            tests=tests,
            mechanisms=mechanisms,
            supporting=supporting,
            chapter=stage.chapter,
            chinese=True,
            stage_number=stage.number,
            test_statement=representative_assertion(patch),
        )
        goal = (
            f"# Stage {stage.number:02d} · {facts.title_en} / {facts.title_zh}\n\n"
            f"<!-- journey: chapter={stage.chapter} tests_added={len(tests)} -->\n\n"
            f"## English\n\n{english}\n\n## 中文\n\n{chinese}\n"
        )
        (directory / "goal.md").write_text(goal)
        (directory / "layout.toml").write_text(
            layout_text(tests, mechanisms, supporting, facts)
        )
    print("wrote 20 bilingual MiniKafka goals and layouts")
    return 0


SLUGS: list[str] = []


if __name__ == "__main__":
    raise SystemExit(main())
