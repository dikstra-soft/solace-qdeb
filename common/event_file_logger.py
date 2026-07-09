"""Append one NDJSON line per event to a log file."""

from __future__ import annotations

from pathlib import Path


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
    def __init__(self, path: str):
        self.path = resolve_log_path(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "a", encoding="utf-8", buffering=1)

    @classmethod
    def open(cls, path: str) -> "EventFileLogger":
        return cls(path)

    def write_line(self, line: str) -> None:
        self._file.write(line + "\n")
        self._file.flush()

    def close(self) -> None:
        try:
            self._file.flush()
            self._file.close()
        except Exception:
            pass
