# Architecture Tour

MiniKafka keeps transport outside the semantic core. Most experiments call the
Direct API, so the main path is visible without sockets:

```text
Producer
  -> keyed partition choice
  -> record batch
  -> leader PartitionLog
  -> follower fetch / ISR
  -> high watermark
  -> Consumer or ConsumerGroup position
```

The storage layer owns append-only segments, sparse offset indexes, CRC
recovery, retention, and keyed compaction. `BrokerCluster` assembles topic
metadata, producer state, group coordination, replica sets, and transaction
visibility. `JsonTcpServer` is only a bounded newline-delimited JSON adapter
over the same operations.

Read the repository in this order:

1. `log/` for batches, segments, indexes, recovery, retention, and compaction.
2. `producer/` for partitioning, batching, acknowledgement, and deduplication.
3. `consumer/` for positions, group ownership, generations, and fencing.
4. `replication/` for follower pulls, ISR, high watermark, epochs, and promotion.
5. `transaction/` for markers, isolation, and atomic output-plus-offset commit.
6. `broker.py`, `cluster.py`, and `lifecycle.py` for ownership and shutdown.
7. `adapters/` last, because transport does not define the core semantics.

The next chapter gives the graded
[MiniKafka → Apache Kafka mapping](kafka-mapping.md). The
[behavior matrix](behavior-matrix.md) connects observable claims to tests.

## Next step

Continue with the [MiniKafka → Apache Kafka mapping](kafka-mapping.md) to
separate source-shaped invariants from deliberate simplifications.
