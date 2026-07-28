from __future__ import annotations


def compaction_observation(
    before_offsets: tuple[int, ...],
    after_offsets: tuple[int, ...],
) -> dict[str, object]:
    return {
        "before_offsets": before_offsets,
        "after_offsets": after_offsets,
        "offsets_renumbered": any(
            offset not in before_offsets for offset in after_offsets
        ),
        "has_gaps": bool(after_offsets)
        and after_offsets != tuple(range(after_offsets[0], after_offsets[-1] + 1)),
    }
