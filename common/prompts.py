"""Interactive prompt helpers with Ctrl+C semantics."""

from __future__ import annotations

import queue
import signal
import sys
import threading
import time
from enum import Enum
from typing import Optional, Union

from common.config import SourceType, parse_topics


class PromptPhase(str, Enum):
    STARTUP = "startup"
    MENU = "menu"
    START_GATE = "start_gate"
    CONSUMING = "consuming"
    PAYLOAD = "payload"


class RuntimeState:
    def __init__(self) -> None:
        self.phase = PromptPhase.STARTUP
        self.quit_requested = False
        self.cancel_requested = False
        self.consume_stop_reason: Optional[str] = None
        self.delete_prompting = False
        self.skip_delete = False
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

    def begin_delete_prompt(self) -> None:
        with self._lock:
            self.delete_prompting = True

    def request_skip_delete(self) -> None:
        with self._lock:
            self.skip_delete = True
            self.cancel_requested = True

    def set_stop_reason(self, reason: str) -> None:
        with self._lock:
            self.consume_stop_reason = reason

    def clear_stop_reason(self) -> None:
        with self._lock:
            self.consume_stop_reason = None

    def interrupt_requested(self) -> bool:
        with self._lock:
            return self.quit_requested or self.cancel_requested


class StdinLinePump:
    """Background stdin reader so Ctrl+C can abort prompts without orphaning ReadLine."""

    def __init__(self) -> None:
        self._lines: queue.Queue[Optional[str]] = queue.Queue()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="stdin-line-pump", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                line = sys.stdin.readline()
            except Exception:
                break
            if self._stop.is_set():
                break
            if line == "":
                # True EOF on redirected stdin. On a TTY, SIGINT can also make
                # readline return "" — exiting here kills the pump and leaves
                # later Mode? prompts waiting forever with no reader.
                if not sys.stdin.isatty():
                    self._lines.put(None)
                    break
                if self._stop.wait(0.01):
                    break
                continue
            self._lines.put(line.rstrip("\n"))

    def try_read(self, timeout: float = 0.05) -> tuple[bool, Optional[str]]:
        try:
            return True, self._lines.get(timeout=timeout)
        except queue.Empty:
            return False, None

    def drain(self) -> None:
        while True:
            try:
                self._lines.get_nowait()
            except queue.Empty:
                break

    def close(self) -> None:
        self._stop.set()


def install_signal_handler(runtime: RuntimeState) -> None:
    def handler(signum, frame):  # noqa: ARG001
        if runtime.phase in (PromptPhase.STARTUP, PromptPhase.MENU):
            runtime.request_quit()
            runtime.cancel_active()
        elif runtime.phase == PromptPhase.START_GATE:
            runtime.cancel_active()
        elif runtime.phase == PromptPhase.CONSUMING:
            runtime.cancel_active()
        elif runtime.phase == PromptPhase.PAYLOAD:
            runtime.request_quit()
            runtime.cancel_active()

    signal.signal(signal.SIGINT, handler)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handler)


def install_publisher_signal_handler(runtime: RuntimeState) -> None:
    """Publisher: Ctrl+C always quits (matches solace-publisher)."""

    def handler(signum, frame):  # noqa: ARG001
        runtime.request_quit()
        runtime.cancel_active()

    signal.signal(signal.SIGINT, handler)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handler)


def install_xfer_signal_handler(runtime: RuntimeState) -> None:
    """Xfer: Ctrl+C during delete prompt skips delete; otherwise quits."""

    def handler(signum, frame):  # noqa: ARG001
        if runtime.delete_prompting:
            runtime.request_skip_delete()
            runtime.cancel_active()
            return
        runtime.request_quit()
        runtime.cancel_active()

    signal.signal(signal.SIGINT, handler)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handler)


def read_line_cancellable(
    runtime: RuntimeState, stdin: Optional[StdinLinePump] = None
) -> Optional[str]:
    if stdin is None:
        # Fallback for callers that have not installed a pump (e.g. early prompts).
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

    while True:
        if runtime.interrupt_requested():
            return None
        got, line = stdin.try_read(0.05)
        if got:
            if runtime.interrupt_requested():
                return None
            return line


def should_abort_prompt(runtime: RuntimeState, value: Optional[str]) -> bool:
    return runtime.interrupt_requested() or value is None


def prompt_source_type(
    default: Union[SourceType, str], runtime: RuntimeState, stdin: StdinLinePump
) -> Optional[SourceType]:
    current = default if isinstance(default, SourceType) else SourceType(str(default))
    while not runtime.quit_requested:
        print(
            f"Dest type? [q]ueue / [t]opic (default {current.value}): ",
            end="",
            flush=True,
        )
        value = read_line_cancellable(runtime, stdin)
        if should_abort_prompt(runtime, value):
            return None
        choice = (value or "").strip().lower()
        if choice == "":
            return current
        if choice in ("q", "queue"):
            return SourceType.QUEUE
        if choice in ("t", "topic"):
            return SourceType.TOPIC
        print("  invalid choice; enter q or t.")
    return None


def prompt_queue(
    default_queue: str, runtime: RuntimeState, stdin: Optional[StdinLinePump] = None
) -> Optional[str]:
    while not runtime.quit_requested:
        prompt = "Queue? " if not default_queue.strip() else f"Queue? (default {default_queue}): "
        print(prompt, end="", flush=True)
        value = read_line_cancellable(runtime, stdin)
        if should_abort_prompt(runtime, value):
            return None
        text = (value or "").strip()
        if text:
            return text
        if default_queue.strip():
            return default_queue
        print("  queue name is required.")
    return None


def prompt_topic(
    default_topic: str, runtime: RuntimeState, stdin: Optional[StdinLinePump] = None
) -> Optional[list[str]]:
    """Prompt for one or more topics (comma-separated). Returns parsed non-empty list."""
    while not runtime.quit_requested:
        prompt = "Topic? " if not default_topic.strip() else f"Topic? (default {default_topic}): "
        print(prompt, end="", flush=True)
        value = read_line_cancellable(runtime, stdin)
        if should_abort_prompt(runtime, value):
            return None
        text = (value or "").strip()
        if text:
            raw = text
        elif default_topic.strip():
            raw = default_topic
        else:
            print("  topic is required (comma-separated list allowed).")
            continue

        topics = parse_topics(raw)
        if not topics:
            print("  at least one non-empty topic is required (comma-separated list allowed).")
            continue
        return topics
    return None


def prompt_yes_no(
    prompt: str,
    default_yes: bool,
    runtime: RuntimeState,
    stdin: Optional[StdinLinePump] = None,
    *,
    ignore_quit: bool = False,
) -> Optional[bool]:
    hint = "[y]/n" if default_yes else "y/[n]"
    while ignore_quit or not runtime.quit_requested:
        if runtime.skip_delete:
            return None
        print(f"{prompt} {hint}: ", end="", flush=True)
        value = read_line_xfer(runtime, stdin, ignore_quit=ignore_quit)
        if runtime.skip_delete:
            return None
        if not ignore_quit and should_abort_prompt(runtime, value):
            return None
        if value is None:
            return None
        choice = value.strip().lower()
        if choice == "":
            return default_yes
        if choice in ("y", "yes"):
            return True
        if choice in ("n", "no"):
            return False
        print("  enter y or n.")
    return None


def read_line_xfer(
    runtime: RuntimeState,
    stdin: Optional[StdinLinePump] = None,
    *,
    ignore_quit: bool = False,
) -> Optional[str]:
    """Like read_line_cancellable, but skip-delete aborts and ignore_quit keeps reading."""
    while True:
        if runtime.skip_delete:
            return None
        if not ignore_quit and runtime.interrupt_requested():
            return None
        if stdin is None:
            return read_line_cancellable(runtime, None)
        got, line = stdin.try_read(0.05)
        if got:
            if runtime.skip_delete:
                return None
            if not ignore_quit and runtime.interrupt_requested():
                return None
            return line


def wait_ms(ms: int, runtime: RuntimeState) -> None:
    until = time.monotonic() + (ms / 1000.0)
    while time.monotonic() < until:
        if runtime.skip_delete or runtime.interrupt_requested():
            return
        time.sleep(0.05)


def throw_if_quit(runtime: RuntimeState) -> None:
    if runtime.interrupt_requested():
        raise KeyboardInterrupt


def prompt_mode(
    default_mode: str, runtime: RuntimeState, stdin: Optional[StdinLinePump] = None
) -> Optional[str]:
    while not runtime.quit_requested:
        print(
            f"Mode? [c]ontinuous / [s]tep / [q]uit (default {default_mode}): ",
            end="",
            flush=True,
        )
        value = read_line_cancellable(runtime, stdin)
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
