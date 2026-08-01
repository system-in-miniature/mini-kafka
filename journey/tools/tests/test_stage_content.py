"""Authored lesson and file-ownership contracts for MiniKafka Stages."""

import re
import tomllib
from pathlib import Path

from journey.tools.extract_history import load_manifest

ROOT = Path(__file__).resolve().parents[3]
STAGES_ROOT = ROOT / "journey" / "stages"
PATCH_HEADER = re.compile(r"^diff --git a/(.+?) b/(.+?)$", re.MULTILINE)
FILE_MARKER = re.compile(r"<!-- journey-file: ([^\n]+) -->")

HEADINGS = {
    "en": (
        "### Goal",
        "### Deliverable files",
        "### The problem at this point",
        "### Test contract",
        "#### See the failure first",
        "### Basic concepts",
        "### Why this mechanism is necessary",
        "### Runtime mental model",
        "### Mechanism blocks",
        "### Verification evidence",
        "### Durable takeaways",
        "### Explain it in your own words",
        "### Textbook",
    ),
    "zh": (
        "### 目标",
        "### 交付文件",
        "### 当前遇到的问题",
        "### 测试契约",
        "#### 先看会坏在哪里",
        "### 基本概念",
        "### 为什么需要这个机制",
        "### 运行时心智模型",
        "### 机制板块",
        "### 验证证据",
        "### 需要真正记住的内容",
        "### 用自己的话讲清楚",
        "### 教材",
    ),
}


def localized(goal: str, language: str) -> str:
    if language == "en":
        return goal[goal.index("## English") : goal.index("## 中文")]
    return goal[goal.index("## 中文") :]


def test_every_stage_has_complete_bilingual_lesson_in_teaching_order() -> None:
    manifest = load_manifest(ROOT / "journey" / "manifest.toml")
    for item in manifest.stages:
        directory = STAGES_ROOT / f"{item.number:02d}-{item.slug}"
        goal = (directory / "goal.md").read_text()
        for language, headings in HEADINGS.items():
            body = localized(goal, language)
            positions = [body.index(heading) for heading in headings]
            assert positions == sorted(positions), (item.number, language)
            test_end = body.index(headings[5])
            failure_position = body.index(headings[4])
            assert body.index(headings[3]) < failure_position < test_end


def test_layout_separates_tests_and_covers_every_changed_file_once() -> None:
    manifest = load_manifest(ROOT / "journey" / "manifest.toml")
    for item in manifest.stages:
        directory = STAGES_ROOT / f"{item.number:02d}-{item.slug}"
        changed = {
            match.group(2)
            for match in PATCH_HEADER.finditer((directory / "stage.patch").read_text())
        }
        data = tomllib.loads((directory / "layout.toml").read_text())
        failure_files = data.get("failure_files", [])
        block_files = [
            path for block in data.get("blocks", []) for path in block["files"]
        ]
        expected_tests = {path for path in changed if path.startswith("tests/")}

        assert set(failure_files) == expected_tests, item.number
        assert not expected_tests & set(block_files), item.number
        assert set(failure_files) | set(block_files) == changed, item.number
        assert len(failure_files) + len(block_files) == len(changed), item.number


def test_each_locale_explains_every_diff_file_without_interview_framing() -> None:
    manifest = load_manifest(ROOT / "journey" / "manifest.toml")
    for item in manifest.stages:
        directory = STAGES_ROOT / f"{item.number:02d}-{item.slug}"
        expected = {
            match.group(2)
            for match in PATCH_HEADER.finditer((directory / "stage.patch").read_text())
        }
        goal = (directory / "goal.md").read_text()
        for language in ("en", "zh"):
            body = localized(goal, language)
            markers = FILE_MARKER.findall(body)
            assert set(markers) == expected, (item.number, language)
            assert len(markers) == len(set(markers)), (item.number, language)
            assert "interview" not in body.lower()
            assert "面试" not in body
