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
    def _read(self, path: Path) -> str:
        self.assertTrue(path.is_file(), path)
        return path.read_text(encoding="utf-8")

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
        english_quickstart = self._read(ENGLISH_CORE[0])
        chinese_quickstart = self._read(CHINESE_CORE[0])
        self.assertIn("> English · [中文快速开始](zh/index.md)", english_quickstart)
        self.assertIn(
            "> [English quick start](../quickstart.md) · 中文快速开始",
            chinese_quickstart,
        )

        for path in ENGLISH_CORE[1:] + CHINESE_CORE[1:]:
            page = self._read(path)
            self.assertNotIn("> English", page, path)
            self.assertNotIn("**Language**", page, path)
            self.assertNotIn("**语言**", page, path)
            self.assertNotIn("· 中文版", page, path)

    def test_core_pages_start_with_a_title_and_end_with_next_step(self) -> None:
        for path in ENGLISH_CORE:
            page = self._read(path)
            self.assertTrue(page.startswith("# "), path)
            self.assertIn("\n## Next step\n", page, path)

        for path in CHINESE_CORE:
            page = self._read(path)
            self.assertTrue(page.startswith("# "), path)
            self.assertIn("\n## 下一步\n", page, path)


if __name__ == "__main__":
    unittest.main()
