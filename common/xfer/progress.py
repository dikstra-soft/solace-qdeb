from __future__ import annotations

import sys


def write(done: int, total: int, chunk: int, chunks: int) -> None:
    pct = 100.0 if total <= 0 else 100.0 * done / total
    line = f"  {pct:5.1f}%  {done}/{total} bytes  chunk {chunk}/{chunks}"
    if not sys.stdout.isatty():
        print(line)
    else:
        print("\r" + line + "    ", end="", flush=True)


def finish() -> None:
    if sys.stdout.isatty():
        print()


def format_bytes(num: int) -> str:
    mib = 1024 * 1024
    if num >= mib:
        return f"{num / mib:.2f}".rstrip("0").rstrip(".") + " MiB"
    if num >= 1024:
        return f"{num / 1024:.2f}".rstrip("0").rstrip(".") + " KiB"
    return f"{num} B"
