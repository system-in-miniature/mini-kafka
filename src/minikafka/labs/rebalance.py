from __future__ import annotations


def rebalance_observation(
    old_generation: int,
    new_generation: int,
) -> dict[str, int | bool]:
    return {
        "old_generation": old_generation,
        "new_generation": new_generation,
        "old_member_fenced": new_generation > old_generation,
    }
