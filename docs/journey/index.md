# Self-Guided Rebuild

Each Stage is a complete independent-browser lesson: understand the current problem, concepts, and necessity; connect related files and critical statements through mechanism blocks; then close with evidence and your own explanation.

This is the browser-based path among MiniKafka's three learning modes. Use the [Mechanism Tutorial](../index.md) for topic-oriented study, or the [Agent-Guided usage guide](../agent-guide.md) for interactive CLI teaching.

For an editor-focused diff, run `python -m journey.tools.build_journey study N` and open `../MiniKafka-journey-workspace`.

| Stage | Topic | New tests | Book chapter |
|---:|---|---:|---:|
| [01](stage-01.md) | Deterministic foundations | 1 | [1](../tutorial/01-getting-started.md) |
| [02](stage-02.md) | Binary-safe record batches | 1 | [2](../tutorial/02-the-log.md) |
| [03](stage-03.md) | Sparse offset lookup | 1 | [2](../tutorial/02-the-log.md) |
| [04](stage-04.md) | Recoverable log segments | 2 | [2](../tutorial/02-the-log.md) |
| [05](stage-05.md) | Segmented partition log | 2 | [2](../tutorial/02-the-log.md) |
| [06](stage-06.md) | Topics and direct administration | 2 | [1](../tutorial/01-getting-started.md) |
| [07](stage-07.md) | Partitioned producer batching | 3 | [4](../tutorial/04-producer.md) |
| [08](stage-08.md) | Consumer position and replay | 3 | [7](../tutorial/07-consumer-groups.md) |
| [09](stage-09.md) | Consumer-group ownership | 3 | [7](../tutorial/07-consumer-groups.md) |
| [10](stage-10.md) | Prefix-only retention | 1 | [3](../tutorial/03-retention-compaction.md) |
| [11](stage-11.md) | Keyed log compaction | 3 | [3](../tutorial/03-retention-compaction.md) |
| [12](stage-12.md) | ISR and high watermark | 3 | [5](../tutorial/05-replication-basics.md) |
| [13](stage-13.md) | Acknowledgement modes | 2 | [6](../tutorial/06-isr-and-fencing.md) |
| [14](stage-14.md) | Promotion and epoch fencing | 3 | [6](../tutorial/06-isr-and-fencing.md) |
| [15](stage-15.md) | Idempotent producer retries | 2 | [4](../tutorial/04-producer.md) |
| [16](stage-16.md) | Transactional records and offsets | 4 | [8](../tutorial/08-transactions.md) |
| [17](stage-17.md) | Thin JSON TCP adapter | 2 | [10](../tutorial/10-protocol-and-beyond.md) |
| [18](stage-18.md) | Lifecycle and failure labs | 2 | [9](../tutorial/09-delivery-semantics.md) |
| [19](stage-19.md) | ISR rejoin regression | 1 | [6](../tutorial/06-isr-and-fencing.md) |
| [20](stage-20.md) | Cross-mechanism domain closure | 5 | [9](../tutorial/09-delivery-semantics.md) |
