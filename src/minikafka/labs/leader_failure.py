from __future__ import annotations


def acknowledged_write_observation(
    *,
    leader_leo: int,
    follower_leo: int,
    high_watermark: int,
) -> dict[str, int | bool]:
    return {
        "leader_leo": leader_leo,
        "follower_leo": follower_leo,
        "high_watermark": high_watermark,
        "leader_only_tail_at_risk": leader_leo > high_watermark,
    }
