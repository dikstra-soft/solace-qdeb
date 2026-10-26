"""Minimal .env loader (cwd first, then beside this package's parent)."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


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


def _search_dirs() -> List[Path]:
    return [Path.cwd(), _script_root()]


def _is_simple_name(value: str) -> bool:
    if not value or value in (".", ".."):
        return False
    p = Path(value)
    if p.is_absolute():
        return False
    return p.parent == Path(".")


def _existing_in_search_dirs(file_name: str) -> Optional[Path]:
    for directory in _search_dirs():
        candidate = directory / file_name
        if candidate.is_file():
            return candidate.resolve()
    return None


def _resolve_explicit(path: str) -> Path:
    """Resolve -e/--env: as-is path, then .env.{value}, then {value} as a filename."""
    tried: List[str] = [path]

    given = Path(path)
    as_is = given.resolve() if given.is_absolute() else (Path.cwd() / given).resolve()
    if as_is.is_file():
        return as_is

    if _is_simple_name(path):
        if not path.startswith(".env"):
            dotted = f".env.{path}"
            tried.append(dotted)
            found = _existing_in_search_dirs(dotted)
            if found is not None:
                return found

        found = _existing_in_search_dirs(path)
        if found is not None:
            return found

    tried_list = ", ".join(f"'{t}'" for t in tried)
    raise FileNotFoundError(f"Env file not found: tried {tried_list}")


def load(path: Optional[str] = None) -> Tuple[Dict[str, str], Optional[str]]:
    """Load .env values. Returns (merged values, path of highest-priority file found).

    When *path* is set (from ``-e`` / ``--env``), it is resolved as a file path
    or short name (``dev`` → ``.env.dev``) and must exist; relative paths
    resolve against the current working directory.
    When *path* is omitted and no ``.env`` is found, a warning is printed to
    stderr and an empty dict is returned (process environment only).
    """
    if path is not None:
        full = _resolve_explicit(path)
        return _parse_env_file(full), str(full)

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

    if loaded_path is None:
        print(
            "WARN: no -e/--env given and no .env file found (cwd or beside binary); using process environment only",
            file=sys.stderr,
        )

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
