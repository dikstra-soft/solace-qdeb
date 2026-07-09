"""Interactive prompt helpers with Ctrl+C semantics."""

from __future__ import annotations

import signal
import sys
import threading
from enum import Enum
from typing import Optional


class PromptPhase(str, Enum):
    STARTUP = "startup"
    MENU = "menu"
    START_GATE = "start_gate"
    CONSUMING = "consuming"


class RuntimeState:
    def __init__(self) -> None:
        self.phase = PromptPhase.STARTUP
        self.quit_requested = False
        self.cancel_requested = False
        self._lock = threading.Lock()

    def request_quit(self) -> None:
        with self._lock:
            self.quit_requested = True
            self.cancel_requested = True

    def cancel_active(self) -> None:
        with self._lock:
            self.cancel_requested = True

    def clear_cancel(self) -> None:
        with self._lock:
            self.cancel_requested = False

    def interrupt_requested(self) -> bool:
        with self._lock:
            return self.quit_requested or self.cancel_requested


def install_signal_handler(runtime: RuntimeState) -> None:
    def handler(signum, frame):  # noqa: ARG001
        if runtime.phase in (PromptPhase.STARTUP, PromptPhase.MENU):
            runtime.request_quit()
            runtime.cancel_active()
        elif runtime.phase == PromptPhase.START_GATE:
            runtime.cancel_active()
        elif runtime.phase == PromptPhase.CONSUMING:
            runtime.cancel_active()

    signal.signal(signal.SIGINT, handler)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handler)


def read_line_cancellable(runtime: RuntimeState) -> Optional[str]:
    result: list[Optional[str]] = [None]
    error: list[Optional[BaseException]] = [None]

    def _read() -> None:
        try:
            line = sys.stdin.readline()
            result[0] = line.rstrip("\n") if line is not None else None
        except Exception as exc:  # pragma: no cover - stdin edge cases
            error[0] = exc

    thread = threading.Thread(target=_read, daemon=True)
    thread.start()
    while thread.is_alive():
        if runtime.interrupt_requested():
            return None
        thread.join(0.05)

    if runtime.interrupt_requested():
        return None
    if error[0] is not None:
        return None
    return result[0]


def should_abort_prompt(runtime: RuntimeState, value: Optional[str]) -> bool:
    return runtime.interrupt_requested() or value is None


def prompt_queue(default_queue: str, runtime: RuntimeState) -> Optional[str]:
    while not runtime.quit_requested:
        prompt = "Queue? " if not default_queue.strip() else f"Queue? (default {default_queue}): "
        print(prompt, end="", flush=True)
        value = read_line_cancellable(runtime)
        if should_abort_prompt(runtime, value):
            return None
        text = (value or "").strip()
        if text:
            return text
        if default_queue.strip():
            return default_queue
        print("  queue name is required.")
    return None


def prompt_topic(default_topic: str, runtime: RuntimeState) -> Optional[str]:
    while not runtime.quit_requested:
        prompt = "Topic? " if not default_topic.strip() else f"Topic? (default {default_topic}): "
        print(prompt, end="", flush=True)
        value = read_line_cancellable(runtime)
        if should_abort_prompt(runtime, value):
            return None
        text = (value or "").strip()
        if text:
            return text
        if default_topic.strip():
            return default_topic
        print("  topic is required.")
    return None


def prompt_mode(default_mode: str, runtime: RuntimeState) -> Optional[str]:
    while not runtime.quit_requested:
        print(
            f"Mode? [c]ontinuous / [s]tep / [q]uit (default {default_mode}): ",
            end="",
            flush=True,
        )
        value = read_line_cancellable(runtime)
        if should_abort_prompt(runtime, value):
            return None
        choice = (value or "").strip().lower()
        if choice == "":
            return default_mode
        if choice in ("c", "continuous"):
            return "continuous"
        if choice in ("s", "step"):
            return "step"
        if choice in ("q", "quit"):
            return None
        print("  invalid choice; enter c, s, or q.")
    return None
