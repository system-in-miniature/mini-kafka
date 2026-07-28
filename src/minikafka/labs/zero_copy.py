from __future__ import annotations

import os
import socket
from pathlib import Path


def sendfile_once(path: Path) -> dict[str, int | bool]:
    left, right = socket.socketpair()
    try:
        with path.open("rb") as source:
            sent = os.sendfile(
                left.fileno(),
                source.fileno(),
                0,
                path.stat().st_size,
            )
        received = len(right.recv(max(1, path.stat().st_size)))
        return {
            "supported": True,
            "sent_bytes": sent,
            "received_bytes": received,
        }
    except (AttributeError, OSError):
        return {"supported": False, "sent_bytes": 0, "received_bytes": 0}
    finally:
        left.close()
        right.close()
