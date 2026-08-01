"""Contracts for history-driven MiniKafka Stage reconstruction."""

import subprocess
from pathlib import Path

from journey.tools import extract_history

ROOT = Path(__file__).resolve().parents[3]


def test_manifest_is_twenty_contiguous_history_driven_stages() -> None:
    manifest = extract_history.load_manifest(ROOT / "journey" / "manifest.toml")

    assert [stage.number for stage in manifest.stages] == list(range(1, 21))
    assert manifest.stages[18].source == "f10ae02"
    assert manifest.stages[-1].slug == "domain-closure"


def test_real_isr_regression_replaces_the_earlier_replica_snapshot() -> None:
    manifest = extract_history.load_manifest(ROOT / "journey" / "manifest.toml")
    before = extract_history.snapshot_for_stage(manifest, 18, root=ROOT)
    after = extract_history.snapshot_for_stage(manifest, 19, root=ROOT)
    path = "src/minikafka/replication/replica_set.py"

    assert before[path] != after[path]
    assert after[path] == extract_history.git_file(ROOT, "f10ae02", path)


def test_final_snapshot_matches_every_owned_reference_byte() -> None:
    manifest = extract_history.load_manifest(ROOT / "journey" / "manifest.toml")
    final = extract_history.snapshot_for_stage(manifest, 20, root=ROOT)
    expected = extract_history.owned_tree(ROOT, manifest)

    assert final == expected


def test_generated_patches_apply_cleanly_and_reach_each_snapshot(
    tmp_path: Path,
) -> None:
    manifest = extract_history.load_manifest(ROOT / "journey" / "manifest.toml")
    workspace = tmp_path / "rebuilt"
    workspace.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=workspace, check=True)

    for stage in manifest.stages:
        patch = extract_history.patch_for_stage(manifest, stage.number, root=ROOT)
        patch_path = tmp_path / f"stage-{stage.number:02d}.patch"
        patch_path.write_bytes(patch)
        subprocess.run(
            ["git", "apply", "--check", str(patch_path)],
            cwd=workspace,
            check=True,
        )
        subprocess.run(["git", "apply", str(patch_path)], cwd=workspace, check=True)
        expected = extract_history.snapshot_for_stage(manifest, stage.number, root=ROOT)
        actual = {
            path.relative_to(workspace).as_posix(): path.read_bytes()
            for path in workspace.rglob("*")
            if path.is_file() and ".git" not in path.parts
        }
        assert actual == expected
