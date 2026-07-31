# MiniKafka Documentation Home and Reference Structure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Separate the MiniKafka site Home from Quick Start and give the five core documentation destinations one consistent structure in English and Chinese.

**Architecture:** Keep one bilingual root Home, move the English setup flow to `quickstart.md`, and retain `zh/index.md` as the Chinese Quick Start for URL compatibility. Enforce navigation, language-banner, and Next-step conventions with a source-level regression test, then verify the rendered MkDocs site in Chromium.

**Tech Stack:** Markdown, MkDocs Material, Python `unittest`, Playwright Chromium.

---

### Task 1: Freeze the new documentation contract

**Files:**
- Create: `tests/test_docs_structure.py`

- [ ] **Step 1: Add the contract test**

Create a standard-library test with these exact core-page lists:

```python
import unittest
from pathlib import Path


ENGLISH_CORE = (
    Path("docs/quickstart.md"),
    Path("docs/architecture.md"),
    Path("docs/kafka-mapping.md"),
    Path("docs/labs-guide.md"),
    Path("docs/behavior-matrix.md"),
)
CHINESE_CORE = (
    Path("docs/zh/index.md"),
    Path("docs/zh/architecture.md"),
    Path("docs/zh/kafka-mapping.md"),
    Path("docs/zh/labs-guide.md"),
    Path("docs/zh/behavior-matrix.md"),
)


class DocumentationStructureTest(unittest.TestCase):
    def test_home_is_separate_from_quick_start(self) -> None:
        home = Path("docs/index.md").read_text(encoding="utf-8")
        nav = Path("mkdocs.yml").read_text(encoding="utf-8")

        self.assertNotIn("uv sync", home)
        self.assertIn("quickstart.md", home)
        self.assertIn("zh/index.md", home)
        self.assertIn("- Home / 首页: index.md", nav)
        self.assertIn("- Quick Start: quickstart.md", nav)
        self.assertIn("- 快速开始: zh/index.md", nav)

    def test_only_quick_starts_have_language_switches(self) -> None:
        english_quickstart = ENGLISH_CORE[0].read_text(encoding="utf-8")
        chinese_quickstart = CHINESE_CORE[0].read_text(encoding="utf-8")
        self.assertIn("> English · [中文快速开始](zh/index.md)", english_quickstart)
        self.assertIn(
            "> [English quick start](../quickstart.md) · 中文快速开始",
            chinese_quickstart,
        )

        for path in ENGLISH_CORE[1:] + CHINESE_CORE[1:]:
            page = path.read_text(encoding="utf-8")
            self.assertNotIn("> English", page, path)
            self.assertNotIn("**Language**", page, path)
            self.assertNotIn("**语言**", page, path)
            self.assertNotIn("· 中文版", page, path)

    def test_core_pages_start_with_a_title_and_end_with_next_step(self) -> None:
        for path in ENGLISH_CORE:
            page = path.read_text(encoding="utf-8")
            self.assertTrue(page.startswith("# "), path)
            self.assertIn("\n## Next step\n", page, path)

        for path in CHINESE_CORE:
            page = path.read_text(encoding="utf-8")
            self.assertTrue(page.startswith("# "), path)
            self.assertIn("\n## 下一步\n", page, path)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Verify RED**

Run: `uv run python tests/test_docs_structure.py -v`

Expected: failures because `docs/quickstart.md` does not exist, Home still contains `uv sync`, reference pages still contain language banners, and Next-step sections are absent.

### Task 2: Separate Home and Quick Start

**Files:**
- Modify: `docs/index.md`
- Create: `docs/quickstart.md`
- Modify: `docs/zh/index.md`
- Modify: `mkdocs.yml`

- [ ] **Step 1: Replace root Home with a bilingual landing page**

The page contains no shell commands. It provides these sections and links:

```markdown
# MiniKafka

MiniKafka is a compact, executable Python model for studying Kafka's core data-plane mechanisms. It favors a direct API and deterministic experiments over wire compatibility.

MiniKafka 是一个紧凑、可运行的 Python 教学模型，用于理解 Kafka 的核心数据平面机制。它优先采用直接 API 与确定性实验，不追求线协议兼容。

## Start here / 从这里开始

- [English quick start](quickstart.md)
- [中文快速开始](zh/index.md)
- [English tutorial contents](tutorial/index.md)
- [中文教程目录](zh/tutorial/index.md)

## Documentation map / 文档地图

| Destination / 页面 | Purpose / 用途 |
| --- | --- |
| Quick Start / 快速开始 | Install MiniKafka and run one failure experiment. / 安装 MiniKafka，并运行第一个故障实验。 |
| Architecture Tour / 架构总览 | Follow one record through storage, replication, and consumption. / 跟踪一条记录如何穿过存储、复制与消费路径。 |
| MiniKafka → Kafka Mapping / 映射 | Separate equivalent mechanisms from deliberate simplifications and semantic opposites. / 区分等价机制、有意简化与语义相反。 |
| Hands-on Labs / 动手实验 | Reproduce acknowledged-write loss and consumer-group rebalance. / 复现已确认写入丢失与消费组再均衡。 |
| Differences and Evidence / 差异与证据 | Bind behavioral claims to executable tests. / 将行为声明绑定到可执行测试证据。 |

## Recommended order / 推荐顺序

Quick Start → Architecture → Mapping → Labs → Differences and Evidence

快速开始 → 架构总览 → 映射 → 动手实验 → 差异与证据

## Scope / 项目边界

MiniKafka is a teaching implementation, not a Kafka-compatible broker. Use the mapping and evidence pages before transferring an observed behavior to production Kafka.

MiniKafka 是教学实现，不是 Kafka 兼容代理。将观察到的行为迁移到生产 Kafka 之前，请先核对映射与证据页面。
```

- [ ] **Step 2: Create the English Quick Start**

Move the English setup and first-experiment material from the old Home into
`docs/quickstart.md`. Start with `# Quick Start`, retain only the banner
`> English · [中文快速开始](zh/index.md)`, and end with:

```markdown
## Next step

Continue with the [Architecture Tour](architecture.md) to see where each part
of the experiment lives in the codebase.
```

- [ ] **Step 3: Normalize the Chinese Quick Start**

Keep its current Chinese setup content, change the title to `# 快速开始`, replace
the banner with `> [English quick start](../quickstart.md) · 中文快速开始`, remove
the English-summary paragraph, and end with:

```markdown
## 下一步

继续阅读[架构总览](architecture.md)，了解实验中的每一步分别落在代码库的
什么位置。
```

- [ ] **Step 4: Update navigation**

Use `Home / 首页` for `index.md`. Add `Quick Start: quickstart.md` directly
under English and `快速开始: zh/index.md` directly under 简体中文. Remove the
old Chinese Quick Start entry from 参考资料.

- [ ] **Step 5: Re-run the focused test**

Run: `uv run python tests/test_docs_structure.py -v`

Expected: the Home/navigation test passes; page-shell tests still fail until Task 3.

### Task 3: Normalize the four reference-page pairs

**Files:**
- Modify: `docs/architecture.md`
- Modify: `docs/kafka-mapping.md`
- Modify: `docs/labs-guide.md`
- Modify: `docs/behavior-matrix.md`
- Modify: `docs/zh/architecture.md`
- Modify: `docs/zh/kafka-mapping.md`
- Modify: `docs/zh/labs-guide.md`
- Modify: `docs/zh/behavior-matrix.md`

- [ ] **Step 1: Remove the eight language banners**

Delete the `English / 中文版`, `Language`, and `语言` blockquotes. Ensure H1 is
the first line of every file; move the current mapping and behavior-matrix H1
above their former banner location by deleting the banner line.

- [ ] **Step 2: Add the English Next-step chain**

Append these destinations in order:

- Architecture → `[MiniKafka → Apache Kafka Mapping](kafka-mapping.md)`
- Mapping → `[Hands-on Labs](labs-guide.md)`
- Labs → `[Differences and Evidence](behavior-matrix.md)`
- Behavior Matrix → `[tutorial contents](tutorial/index.md)`

Every section uses the exact heading `## Next step` and one sentence explaining
why to continue.

- [ ] **Step 3: Add the Chinese Next-step chain**

Append the corresponding destinations:

- 架构总览 → `[MiniKafka → Apache Kafka 映射](kafka-mapping.md)`
- 映射 → `[动手实验](labs-guide.md)`
- 动手实验 → `[差异与证据](behavior-matrix.md)`
- 行为矩阵 → `[教程目录](tutorial/index.md)`

Every section uses the exact heading `## 下一步` and one sentence explaining
why to continue.

- [ ] **Step 4: Verify GREEN**

Run: `uv run python tests/test_docs_structure.py -v`

Expected: all three structure tests pass.

### Task 4: Full and rendered acceptance

**Files:**
- Test all files changed above.

- [ ] **Step 1: Run the complete suite and strict build**

Run: `uv run pytest -q`

Expected: all tests pass.

Run: `uvx --from 'mkdocs-material>=9.5,<10' mkdocs build --strict --site-dir /tmp/codex-minikafka-docs-structure`

Expected: exit zero.

- [ ] **Step 2: Inspect the rendered site**

Use Chromium to open the built root Home, English Quick Start, Chinese Quick
Start, and Mapping pages at 1280px and 390px. Assert the root page has no
`uv sync`, both quick starts show one language switch, reference pages show
none, and the navigation exposes Home plus both Quick Start entries.

- [ ] **Step 3: Review and commit**

Run `git diff --check`, inspect the complete diff, then commit only the scoped
documentation, test, spec, and plan files with:

```bash
git commit -m "docs: separate home from quick start"
```
