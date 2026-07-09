"""Minimal .env loader (cwd first, then beside this package's parent)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional, Tuple


def _script_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _unquote(value: str) -> str:
    if len(value) >= 2 and (
        (value[0] == '"' and value[-1] == '"')
        or (value[0] == "'" and value[-1] == "'")
    ):
        return value[1:-1]
    return value


def _parse_env_file(path: Path) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        eq = line.find("=")
        if eq <= 0:
            continue
        key = line[:eq].strip()
        value = _unquote(line[eq + 1 :].strip())
        if key:
            result[key] = value
    return result


def load() -> Tuple[Dict[str, str], Optional[str]]:
    """Load .env values. Returns (merged values, path of highest-priority file found)."""
    cwd_file = Path.cwd() / ".env"
    script_file = _script_root() / ".env"

    merged: Dict[str, str] = {}
    loaded_path: Optional[str] = None

    if script_file.is_file():
        merged.update(_parse_env_file(script_file))
        loaded_path = str(script_file)

    if cwd_file.is_file():
        merged.update(_parse_env_file(cwd_file))
        loaded_path = str(cwd_file)

    return merged, loaded_path


def get(
    key: str,
    env_file: Dict[str, str],
    *,
    required: bool = False,
    default: Optional[str] = None,
) -> Optional[str]:
    value = env_file.get(key)
    if value is not None and value.strip():
        return value.strip()
    sys_val = os.environ.get(key)
    if sys_val is not None and sys_val.strip():
        return sys_val.strip()
    if required and default is None:
        raise ValueError(
            f"Missing required setting '{key}'. Set it in .env or the environment."
        )
    return default
