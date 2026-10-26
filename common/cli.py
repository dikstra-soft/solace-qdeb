"""Shared command-line parsing for consumer, browser, publisher, and xfer."""

from __future__ import annotations

import sys
from typing import List, Optional, Tuple

ENV_USAGE = "Usage: -e <file-or-name>  or  --env <file-or-name>"


def parse_cli(
    argv: Optional[List[str]] = None,
) -> Tuple[Optional[str], List[str], Optional[str]]:
    """Extract -e/--env and leftover positional args from argv.

    Returns (path_or_name, remaining, error). Value is a file path or short
    name (e.g. ``dev`` → ``.env.dev``), or None when the flag is omitted.
    On usage error, remaining is empty and error is a message to print.
    """
    args = sys.argv[1:] if argv is None else argv
    env_file: Optional[str] = None
    leftover: List[str] = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in ("-e", "--env"):
            if i + 1 >= len(args) or args[i + 1] in ("-e", "--env"):
                return None, [], (
                    f"-e/--env requires a file path or short name (e.g. dev → .env.dev).\n{ENV_USAGE}"
                )
            value = args[i + 1]
            if not value.strip():
                return None, [], (
                    f"-e/--env requires a file path or short name (e.g. dev → .env.dev).\n{ENV_USAGE}"
                )
            env_file = value
            i += 2
            continue
        leftover.append(arg)
        i += 1
    return env_file, leftover, None


def parse_env_file(argv: Optional[List[str]] = None) -> Tuple[Optional[str], Optional[str]]:
    """Extract -e/--env from argv (defaults to sys.argv[1:]).

    Returns (path_or_name, error). Value is a file path or short name
    (e.g. ``dev`` → ``.env.dev``), or None when the flag is omitted.
    On usage error, path is None and error is a message to print.
    """
    env_file, _remaining, error = parse_cli(argv)
    return env_file, error
