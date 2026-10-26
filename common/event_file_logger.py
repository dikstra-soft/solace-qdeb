"""Append one line per event to a log file (NDJSON, simple, or payload-only)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from common.config import LogFormat
from common import payload_formatter


def _script_root() -> Path:
    return Path(__file__).resolve().parent.parent


def resolve_log_path(path: str) -> str:
    """Resolve relative log paths using the same .env location priority as dotenv."""
    candidate = Path(path)
    if candidate.is_absolute():
        return str(candidate.resolve())

    if (Path.cwd() / ".env").is_file():
        return str((Path.cwd() / path).resolve())

    script_root = _script_root()
    if (script_root / ".env").is_file():
        return str((script_root / path).resolve())

    return str((Path.cwd() / path).resolve())


class EventFileLogger:
    def __init__(self, path: str, log_format: LogFormat = LogFormat.FULL):
        self.path = resolve_log_path(path)
        self.log_format = log_format
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "a", encoding="utf-8", buffering=1)

    @classmethod
    def open(cls, path: str, log_format: LogFormat = LogFormat.FULL) -> "EventFileLogger":
        return cls(path, log_format)

    def write_line(self, line: str) -> None:
        self._file.write(line + "\n")
        self._file.flush()

    def write_event(self, message: Any) -> None:
        self.write_line(payload_formatter.build_log_line(message, self.log_format))

    def close(self) -> None:
        try:
            self._file.flush()
            self._file.close()
        except Exception:
            pass
