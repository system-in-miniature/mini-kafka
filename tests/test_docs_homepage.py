import re
import unittest
from pathlib import Path


class DocumentationHomepageTest(unittest.TestCase):
    def test_homepage_is_a_bilingual_landing_page(self) -> None:
        homepage = Path("docs/index.md").read_text(encoding="utf-8")
        headings = [line for line in homepage.splitlines() if line.startswith("#")]

        self.assertTrue(headings)
        self.assertEqual(headings[0], "# MiniKafka")
        self.assertTrue(all(" / " in heading for heading in headings[1:]))
        self.assertIn("[English quick start](quickstart.md)", homepage)
        self.assertIn("[中文快速开始](zh/index.md)", homepage)
        self.assertGreaterEqual(len(re.findall(r"[\u4e00-\u9fff]", homepage)), 120)


if __name__ == "__main__":
    unittest.main()
