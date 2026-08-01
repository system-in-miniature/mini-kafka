# MiniKafka Polished Learning Surfaces Design

## Objective

MiniKafka already has a bilingual ten-chapter mechanism tutorial and a verified
reference implementation. It becomes polished by adding the same three learning
modes as MiniS3 while preserving MiniKafka's own history and causal boundaries:

1. **Mechanism Tutorial** — the existing ten topic-oriented chapters.
2. **Self-Guided Rebuild** — twenty bilingual browser-native Stages backed by an
   exact cumulative patch chain.
3. **Agent-Guided Rebuild** — direct `开始 Agent 带教 Stage NN` routing from the
   canonical repository, with no learner branch switching.

## Alternatives considered

A ten-Stage Journey aligned one-to-one with the tutorial chapters would make
navigation compact, but storage, replication, and transaction chapters would
each contain several unrelated implementation deltas. A fully hand-authored
Journey could make every Stage equally sized, but would discard the repository's
unusually clean mechanism-by-mechanism history.

The selected approach follows the twenty real source-bearing boundaries, with a
small final consolidation for public API and cross-mechanism corrections. It
keeps diffs readable, preserves real regression stories, and lets the existing
tutorial remain the higher-level conceptual map.

## Stage map

| Stage | Historical boundary | Mechanism increment |
|---:|---|---|
| 01 | `53ca574` | Deterministic time, configuration, and typed failures |
| 02 | `37b902f` | Binary-safe record batches, framing, CRC, and corruption detection |
| 03 | `44bb3df` | Sparse offset index and floor lookup |
| 04 | `20861e5` | Recoverable segments and incomplete-tail truncation |
| 05 | `5a79c70` | Segmented partition log, rolling, restart, and reads |
| 06 | `09d30dc` | Topic metadata, replica assignment, cluster, and direct administration |
| 07 | `f74516b` | Partitioning, bounded accumulation, linger, batching, and ordering |
| 08 | `70b6bd9` | Consumer position, committed offset, reset policy, and replay |
| 09 | `09c6ab9` | Group membership, assignment, heartbeat, rebalance, and generation fencing |
| 10 | `066472b` | Time- and size-based prefix retention |
| 11 | `bfb18a8` | Keyed compaction, tombstones, offset gaps, and atomic swap |
| 12 | `5c8634e` | Follower pull, ISR, high watermark, and committed visibility |
| 13 | `c94e229` | `acks=0`, `acks=1`, `acks=all`, min ISR, and acknowledgement failure windows |
| 14 | `c758fa1` | Promotion, leader epochs, fencing, and divergent-tail truncation |
| 15 | `40cf820` | Producer identity, epochs, sequence numbers, retry deduplication, and recovery |
| 16 | `590005b` | Transaction journal, control batches, atomic offsets, abort, and recovery |
| 17 | `cb6fcc3` | Length-prefixed JSON TCP translation and Direct API parity |
| 18 | `080df0e` plus current lab slice | Lifecycle ownership, background failure propagation, shutdown, and runnable failure labs |
| 19 | `f10ae02` | Regression: replicas behind the high watermark cannot rejoin ISR or lead |
| 20 | `684d82f..HEAD` selected source/test files | Public domain closure plus transactional `acks=all`, prefix-only retention, replica-level `read_committed`, and final source parity |

## Canonical artifacts

Each `journey/stages/NN-slug/` contains:

- `goal.md` — bilingual authored lesson facts;
- `stage.patch` — exact delta from Stage N-1;
- `tests.txt` — cumulative focused executable evidence;
- `layout.toml` — test-file ownership plus grouped mechanism/supporting blocks.

`journey/manifest.toml` records the source commit or selected historical slice
used to reconstruct each cumulative boundary. Stage 20 must reproduce every
owned source and test byte in the current reference tree.

## Teaching order and content ownership

Every localized browser page follows this order:

1. current problem;
2. **Test contract**, containing the nested failure preview, collapsed test
   diffs, counterexample construction, critical assertion, and failure meaning;
3. basic concepts and necessity;
4. runtime mental model;
5. grouped mechanism blocks with implementation diffs and key statements;
6. verification evidence, durable takeaways, and learner explanation;
7. relevant tutorial chapter.

Failure previews are test content and are authored directly inside the Test
contract. Tests never appear inside mechanism blocks. Multiple files that
implement one causal boundary share one block title and may share one
explanation. Routine package markers, lockfiles, configuration, and public
export wiring stay in collapsed supporting groups rather than receiving
individual explanations.

Tests are executable motivation and completion evidence, not a mandatory
test-first teaching narrative.

## Patch and parity boundary

The Journey owns:

- `src/minikafka/**`;
- behavioral tests under `tests/unit`, `tests/log`, `tests/producer`,
  `tests/consumer`, `tests/replication`, `tests/transaction`,
  `tests/reliability`, and `tests/adapters`;
- `tests/test_direct_cluster.py`, `tests/test_final_acceptance.py`, and
  `tests/test_sloc_report.py`;
- `pyproject.toml` and `uv.lock`.

It excludes the existing tutorial/reference documentation, website assets, and
documentation-structure tests. Those remain normal repository surfaces and are
verified separately by the full suite and strict MkDocs build.

## Agent contract

The root `AGENTS.md` routes an explicit Stage request, prepares or resumes a
Stage-specific internal learner repository, gives a short misconception screen,
teaches concepts before implementation, uses tests as visible evidence, walks
the authored mechanism blocks, and verifies both tests and canonical parity.
The learner asks from the canonical checkout; the tooling owns the internal
workspace, and the learner never switches a teaching branch. Web Agent pages
remain short usage guides only.

## Acceptance gates

MiniKafka is polished only when all are freshly green:

- full repository tests, Ruff, and compileall;
- all twenty cumulative Stage checks;
- final owned-tree byte parity;
- bilingual renderer coverage with no unowned or duplicated diff;
- generated-page drift check and strict MkDocs build;
- real-browser checks across representative storage, producer, replication,
  consumer, transaction, protocol, regression, and final Stages in both
  languages, including collapsed drawers, test-before-concept order,
  same-Stage language preservation, and Agent routing;
- a clean feature worktree containing local commits only.
