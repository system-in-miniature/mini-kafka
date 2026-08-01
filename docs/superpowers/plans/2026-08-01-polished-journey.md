# MiniKafka Polished Journey Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a twenty-Stage bilingual Self-Guided Rebuild and direct
Agent-Guided Rebuild to MiniKafka while preserving the existing mechanism
tutorial and proving final source parity.

**Architecture:** A manifest-driven extractor reconstructs cumulative Stage
trees from real Git boundaries and selected final-tree slices. Checked-in
patches, focused test lists, bilingual authored goals, and grouped layouts are
the canonical learning artifacts; deterministic render/build tools turn them
into browser pages and resumable Agent workspaces.

**Tech Stack:** Python 3.12, pytest, TOML, Git patches, MkDocs Material, uv,
Playwright for browser acceptance.

---

### Task 1: Freeze the extraction and parity contract

**Files:**

- Create: `journey/manifest.toml`
- Create: `journey/tools/__init__.py`
- Create: `journey/tools/extract_history.py`
- Create: `journey/tools/tests/__init__.py`
- Create: `journey/tools/tests/test_extract_history.py`
- Modify: `tests/test_docs_structure.py`

- [ ] Fix the baseline import-order violation in
  `tests/test_docs_structure.py` and prove `uv run ruff check .` is green.
- [ ] Write extractor tests that require 20 ordered Stage entries, resolvable
  historical boundaries, deterministic patch output, clean cumulative patch
  application, and Stage 20 byte parity for every owned file.
- [ ] Run `uv run pytest -q journey/tools/tests/test_extract_history.py` and
  confirm it fails because the manifest and extractor do not exist.
- [ ] Implement explicit manifest parsing, Git tree extraction, selected-file
  overlays, deletion handling, unified patch generation, and parity reporting.
- [ ] Run the extractor tests and `git diff --check`.
- [ ] Commit the baseline fix and extraction contract.

### Task 2: Generate the twenty canonical patch boundaries

**Files:**

- Create: `journey/stages/01-primitives/stage.patch`
- Create: `journey/stages/02-record-batches/stage.patch`
- Create: `journey/stages/03-offset-index/stage.patch`
- Create: `journey/stages/04-recoverable-segments/stage.patch`
- Create: `journey/stages/05-partition-log/stage.patch`
- Create: `journey/stages/06-topic-administration/stage.patch`
- Create: `journey/stages/07-producer-batching/stage.patch`
- Create: `journey/stages/08-consumer-positions/stage.patch`
- Create: `journey/stages/09-consumer-groups/stage.patch`
- Create: `journey/stages/10-retention/stage.patch`
- Create: `journey/stages/11-compaction/stage.patch`
- Create: `journey/stages/12-isr-high-watermark/stage.patch`
- Create: `journey/stages/13-ack-modes/stage.patch`
- Create: `journey/stages/14-promotion-fencing/stage.patch`
- Create: `journey/stages/15-idempotent-producer/stage.patch`
- Create: `journey/stages/16-transactions/stage.patch`
- Create: `journey/stages/17-json-tcp/stage.patch`
- Create: `journey/stages/18-lifecycle-labs/stage.patch`
- Create: `journey/stages/19-isr-regression/stage.patch`
- Create: `journey/stages/20-domain-closure/stage.patch`
- Create: matching `tests.txt` files in all twenty Stage directories
- Create: `journey/tools/tests/test_stage_contracts.py`

- [ ] Define focused cumulative pytest paths for every Stage so a learner sees
  only evidence supported by the current cumulative tree.
- [ ] Write contract tests for non-empty patches, cumulative applicability,
  valid test paths, chronological ownership, and exact final parity.
- [ ] Run the contract test first and observe missing Stage artifacts.
- [ ] Extract Stages 01–17 from their source commits; reconstruct Stage 18 from
  the lifecycle boundary plus current surviving lab files; preserve the real
  ISR regression in Stage 19; overlay final owned files in Stage 20.
- [ ] Apply all patches into a temporary repository and run every cumulative
  `tests.txt` command.
- [ ] Commit the complete patch chain.

### Task 3: Author bilingual lesson facts and grouped layouts

**Files:**

- Create: `journey/stages/*/goal.md`
- Create: `journey/stages/*/layout.toml`
- Create: `journey/tools/tests/test_stage_content.py`

- [ ] Write content tests requiring English and Chinese sections, a Test
  contract with nested failure preview, concepts before mechanisms, key test
  statements, key production statements, verification commands, durable
  takeaways, and one relevant tutorial link per Stage.
- [ ] Require each changed test file to appear only in the Test contract and
  each production file to be owned by exactly one mechanism or supporting
  group; allow consecutive files to share one causal explanation.
- [ ] Run the content tests and observe all missing goals/layouts.
- [ ] Author Stages 01–05 for primitives and the append-only storage substrate.
- [ ] Author Stages 06–11 for administration, producer, consumer, retention,
  and compaction.
- [ ] Author Stages 12–17 for replication, acknowledgement, promotion,
  idempotence, transactions, and protocol translation.
- [ ] Author Stages 18–20 for lifecycle, the ISR counterexample, and final
  cross-mechanism corrections.
- [ ] Run content/ownership tests and commit the authored Stage corpus.

### Task 4: Render deterministic bilingual browser lessons

**Files:**

- Create: `journey/tools/render_pages.py`
- Create: `journey/tools/tests/test_render_pages.py`
- Create: generated `docs/journey/index.md`
- Create: generated `docs/journey/stage-01.md` through `stage-20.md`
- Create: generated `docs/zh/journey/index.md`
- Create: generated `docs/zh/journey/stage-01.md` through `stage-20.md`

- [ ] Write renderer tests for all 42 generated pages, stable regeneration,
  bilingual structural parity, collapsed deliverables, collapsed diff drawers,
  same-group multi-file rendering, and test-before-concept ordering.
- [ ] Add a regression assertion that failure previews render inside Test
  contracts and no test diff appears beneath mechanism headings.
- [ ] Run renderer tests and observe missing output.
- [ ] Adapt the proven MiniDist renderer to MiniKafka's manifest schema and
  twenty-Stage content while keeping the authored-source contract intact.
- [ ] Generate both locales twice and require a clean second diff.
- [ ] Commit renderer, tests, and generated pages.

### Task 5: Add resumable Self-Guided and Agent-Guided tooling

**Files:**

- Create: `journey/tools/build_journey.py`
- Create: `journey/tools/tests/test_build_journey.py`
- Create: `journey/tools/tests/test_agent_mode.py`
- Create: `AGENTS.md`
- Create: `docs/agent-guide.md`
- Create: `docs/zh/agent-guide.md`

- [ ] Write tests for `study`, `attempt`, `agent`, and `check`, including clean
  Stage N-1 setup, protected reset, Stage-specific internal repositories,
  `READY`/`RESUME`, no teaching branch, exact cumulative tests, and source
  parity.
- [ ] Run tests and observe the missing command surface.
- [ ] Implement deterministic workspace setup and verification without
  modifying or switching the learner's canonical branch.
- [ ] Add a root Agent contract that performs a brief misconception screen,
  teaches the authored concepts, exposes tests as evidence, walks mechanism
  groups, and checks exact parity.
- [ ] Keep both Agent web pages to a short usage tutorial only.
- [ ] Run tooling tests and a real Stage 12 prepare/resume/check cycle.
- [ ] Commit both learning modes.

### Task 6: Integrate navigation, localization, and CI

**Files:**

- Modify: `README.md`
- Modify: `README.zh-CN.md`
- Modify: `docs/index.md`
- Modify: `docs/zh/index.md`
- Modify: `mkdocs.yml`
- Modify: `.github/workflows/ci.yml`
- Create or modify: `docs/assets/javascripts/language-switch.js`
- Modify: `journey/tools/tests/test_render_pages.py`

- [ ] Write failing navigation tests for three clearly named modes, all 20
  Stage routes in both locales, short Agent guides, and same-Stage language
  switching.
- [ ] Add concise mode cards/links without replacing the existing tutorial or
  reference surfaces.
- [ ] Register all pages and the same-path locale switch in MkDocs.
- [ ] Add CI commands for Journey tooling, generated drift, all Stage checks,
  full tests, Ruff, and strict MkDocs.
- [ ] Run navigation tests, generated drift, and `uv run mkdocs build --strict`.
- [ ] Commit site integration.

### Task 7: Perform polished acceptance

**Files:**

- Modify only files required by defects exposed during acceptance.

- [ ] Run `uv run pytest -q`, `uv run ruff check .`, and
  `uv run python -m compileall -q src tests journey`.
- [ ] Run every Journey tooling test and all 20 cumulative Stage checks.
- [ ] Regenerate pages and require `git diff --exit-code`.
- [ ] Prove final owned-tree byte parity against the current reference tree.
- [ ] Build the documentation with strict warnings-as-errors behavior.
- [ ] Serve the site locally and use a real browser to inspect representative
  Stages 01, 04, 07, 09, 12, 14, 16, 17, 19, and 20 in both languages.
- [ ] Verify all drawers start collapsed, the visible order is Test contract →
  failure preview → concepts → mechanisms, language switching preserves the
  Stage path, and both Agent guide routes work.
- [ ] Remove generated build artifacts from the worktree, commit any acceptance
  fixes, and require `git status --short` to be empty.
