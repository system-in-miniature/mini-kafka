from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Header:
    key: str
    value: bytes | None


@dataclass(frozen=True, slots=True)
class Record:
    key: bytes | None
    value: bytes | None
    timestamp_ms: int
    headers: tuple[Header, ...] = ()


@dataclass(frozen=True, slots=True)
class StoredRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int
    headers: tuple[Header, ...] = ()


@dataclass(frozen=True, slots=True)
class LogRecord:
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int
    headers: tuple[Header, ...] = ()
