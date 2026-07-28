from __future__ import annotations


def delivery_observation(*, commit_before_processing: bool) -> dict[str, str]:
    return {
        "mode": (
            "at-most-once" if commit_before_processing else "at-least-once"
        ),
        "failure": (
            "message-loss"
            if commit_before_processing
            else "duplicate-processing"
        ),
    }
