from pathlib import Path

from tools.count_sloc import count_tree


def test_sloc_report_counts_code_tests_and_docs() -> None:
    report = count_tree(Path.cwd())
    assert report["production"] > 0
    assert report["tests"] > 0
    assert report["docs"] > 0
