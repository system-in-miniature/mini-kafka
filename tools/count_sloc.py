from __future__ import annotations

from pathlib import Path


def _lines(paths: list[Path]) -> int:
    return sum(
        sum(1 for line in path.read_text().splitlines() if line.strip())
        for path in paths
    )


def count_tree(root: Path) -> dict[str, int]:
    return {
        "production": _lines(list((root / "src").rglob("*.py"))),
        "tests": _lines(list((root / "tests").rglob("*.py"))),
        "docs": _lines(
            [root / "README.md"]
            + list((root / "docs").rglob("*.md"))
        ),
    }


if __name__ == "__main__":
    for category, lines in count_tree(Path.cwd()).items():
        print(f"{category}: {lines}")
