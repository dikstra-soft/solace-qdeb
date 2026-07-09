#!/usr/bin/env python3
"""solace-browser — SMF queue browser (non-destructive)."""

from __future__ import annotations

import re
import signal
import sys
from enum import Enum
from typing import List, Optional, Tuple

from solace.messaging.receiver.message_receiver import InboundMessage

from common import payload_formatter
from common.config import BrowserAppConfig
from common.connection import SolaceConnection
from common.event_file_logger import EventFileLogger
from common.queue_browser import QueueBrowser

QUIT_REQUESTED = False


def _install_quit_handler() -> None:
    def handler(signum, frame):  # noqa: ARG001
        global QUIT_REQUESTED
        QUIT_REQUESTED = True

    signal.signal(signal.SIGINT, handler)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handler)


def prompt_queue_name(default_queue: str) -> str:
    while True:
        prompt = "Queue? " if not default_queue.strip() else f"Queue? (default {default_queue}): "
        value = input(prompt).strip()
        if value:
            return value
        if default_queue.strip():
            return default_queue
        print("  queue name is required.")


def prompt_page_size(default_page_size: int) -> int:
    while True:
        value = input(f"Page size? (default {default_page_size}): ").strip()
        if not value:
            return default_page_size
        try:
            size = int(value)
        except ValueError:
            print("  page size must be a positive integer.")
            continue
        if size > 0:
            return size
        print("  page size must be a positive integer.")


def prompt_new_page_size(current_page_size: int) -> Tuple[bool, int]:
    while True:
        value = input(f"New page size? (current {current_page_size}): ").strip()
        if not value:
            return False, current_page_size
        try:
            size = int(value)
        except ValueError:
            print("  page size must be a positive integer.")
            continue
        if size > 0:
            return True, size
        print("  page size must be a positive integer.")


def total_pages(total_count: int, page_size: int) -> int:
    if total_count <= 0:
        return 1
    return (total_count + page_size - 1) // page_size


def clamp_page(page: int, total_count: int, page_size: int) -> int:
    return max(1, min(page, total_pages(total_count, page_size)))


def first_item_on_page(page: int, page_size: int, total_count: int) -> int:
    first = (page - 1) * page_size + 1
    return min(first, max(total_count, 1))


def last_item_on_page(page: int, page_size: int, total_count: int) -> int:
    if total_count == 0:
        return 0
    return min(page * page_size, total_count)


def render_page_info(queue_name: str, page: int, page_size: int, total_count: int) -> None:
    pages = total_pages(total_count, page_size)
    if total_count == 0:
        item_range = "none"
    else:
        item_range = f"{first_item_on_page(page, page_size, total_count)}-{last_item_on_page(page, page_size, total_count)}"
    print()
    print(
        f"queue: {queue_name}  |  page {page}/{pages}  |  "
        f"items {item_range} of {total_count}  |  page size {page_size}"
    )


def render_page_items(messages: List[InboundMessage], page: int, page_size: int) -> None:
    if not messages:
        print("(no messages on this page)")
        return
    for i, message in enumerate(messages):
        absolute_item = (page - 1) * page_size + i + 1
        print(payload_formatter.format_compact_browser_line(message, absolute_item), end="")
    sys.stdout.flush()


def pause_for_continue() -> None:
    if QUIT_REQUESTED:
        return
    try:
        input("press Enter to continue... ")
    except (EOFError, KeyboardInterrupt):
        pass


def print_help(detailed: bool = False) -> None:
    if detailed:
        print(
            "Commands:\n"
            "  <n>         go to page n\n"
            "  n / p       next / previous page\n"
            "  s / size    change page size (returns to page 1)\n"
            "  r           refresh (rescan queue, page 1)\n"
            "  i<n>        full dump of absolute item #n (e.g. i5)\n"
            "  d           full dump of current page (console + log file)\n"
            "  h           this help\n"
            "  q           quit\n"
            "Item #1 is the newest message. Browsing is non-destructive.\n"
            "Total count comes from a full oldest-first scan (may be slow on large queues)."
        )
    else:
        print("Commands: <n>, n/p, s size, i<N>, d dump page, r refresh, h help, q quit")


class BrowseAction(str, Enum):
    QUIT = "quit"
    HELP = "help"
    REFRESH = "refresh"
    NEXT_PAGE = "next"
    PREV_PAGE = "prev"
    GO_TO_PAGE = "goto"
    DUMP_PAGE = "dump"
    DUMP_ITEM = "dump_item"
    CHANGE_PAGE_SIZE = "size"


def try_parse_command(command: str) -> Tuple[Optional[BrowseAction], int]:
    lower = command.lower()
    mapping = {
        "q": BrowseAction.QUIT,
        "quit": BrowseAction.QUIT,
        "exit": BrowseAction.QUIT,
        "h": BrowseAction.HELP,
        "help": BrowseAction.HELP,
        "?": BrowseAction.HELP,
        "r": BrowseAction.REFRESH,
        "refresh": BrowseAction.REFRESH,
        "n": BrowseAction.NEXT_PAGE,
        "next": BrowseAction.NEXT_PAGE,
        "p": BrowseAction.PREV_PAGE,
        "prev": BrowseAction.PREV_PAGE,
        "previous": BrowseAction.PREV_PAGE,
        "d": BrowseAction.DUMP_PAGE,
        "dump": BrowseAction.DUMP_PAGE,
        "s": BrowseAction.CHANGE_PAGE_SIZE,
        "size": BrowseAction.CHANGE_PAGE_SIZE,
    }
    if lower in mapping:
        return mapping[lower], 0

    item_match = re.fullmatch(r"i(\d+)", lower)
    if item_match:
        return BrowseAction.DUMP_ITEM, int(item_match.group(1))

    item_prefix = re.fullmatch(r"item\s+(\d+)", command, flags=re.IGNORECASE)
    if item_prefix:
        return BrowseAction.DUMP_ITEM, int(item_prefix.group(1))

    if lower.isdigit():
        return BrowseAction.GO_TO_PAGE, int(lower)

    return None, 0


def rescan(
    browser: QueueBrowser,
    queue_name: str,
    page_size: int,
) -> Tuple[int, List[InboundMessage], int]:
    print()
    print(f"Scanning queue '{queue_name}' (oldest-first pass to count messages)...")
    total = browser.scan_total_count()
    print(f"Found {total} message{'s' if total != 1 else ''}.")
    current_page = 1
    current_messages = browser.fetch_page(1, page_size, total) if total > 0 else []
    return total, current_messages, current_page


def go_to_page(
    browser: QueueBrowser,
    page: int,
    page_size: int,
    total_count: int,
) -> List[InboundMessage]:
    if total_count <= 0:
        return []
    return browser.fetch_page(page, page_size, total_count)


def dump_page(
    messages: List[InboundMessage],
    page: int,
    page_size: int,
    file_logger: Optional[EventFileLogger],
    *,
    log_to_file: bool,
) -> bool:
    items = list(messages)
    if not items:
        print("  (empty page — nothing to dump)")
        return False

    for i, message in enumerate(items):
        absolute_item = (page - 1) * page_size + i + 1
        print(payload_formatter.format_full_console(message, absolute_item), end="")
    sys.stdout.flush()

    if file_logger is None:
        if log_to_file:
            print("  (log file unavailable — open failed at startup; console dump only)")
        else:
            print("  (log file disabled — set SOLACE_LOG_TO_FILE=true to persist page dumps)")
        return True

    logged = 0
    errors: List[str] = []
    try:
        file_logger.write_line("")
        file_logger.write_line("--- solace-browser page dump ---")
        for message in items:
            try:
                file_logger.write_line(payload_formatter.build_json_line(message))
                logged += 1
            except Exception as exc:
                errors.append(str(exc))
        file_logger.write_line("--- solace-browser page dump ---")
        file_logger.write_line("")
    except Exception as exc:
        print(f"  [logfile] write failed: {exc}", file=sys.stderr)
        return True

    if errors:
        print(
            f"  dumped {logged}/{len(items)} item(s) to {file_logger.path} "
            f"({len(errors)} failed — see stderr)",
            file=sys.stderr if logged == 0 else sys.stdout,
        )
        for err in errors[:3]:
            print(f"  [logfile] item write failed: {err}", file=sys.stderr)
        if len(errors) > 3:
            print(f"  [logfile] ... and {len(errors) - 3} more", file=sys.stderr)
    else:
        print(f"  dumped {logged} item(s) to {file_logger.path}")

    return True


def dump_item(browser: QueueBrowser, absolute_item: int, total_count: int) -> bool:
    if absolute_item < 1 or absolute_item > total_count:
        print(f"  item #{absolute_item} is out of range (1..{total_count}).")
        return False
    message = browser.fetch_item(absolute_item, total_count)
    if message is None:
        print(f"  could not read item #{absolute_item}.")
        return False
    print(payload_formatter.format_full_console(message, absolute_item), end="")
    sys.stdout.flush()
    return True


def main() -> int:
    global QUIT_REQUESTED

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    print("solace-browser  (SMF queue browser, non-destructive)")
    print("---------------------------------------------------")

    try:
        config, env_path = BrowserAppConfig.load()
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        print("Create a .env file (see .env.example).", file=sys.stderr)
        return 1

    if env_path is None:
        print("config source : process environment (no .env file found)")
    else:
        print(f"config source : {env_path}")

    _install_quit_handler()

    queue_name = prompt_queue_name(config.default_queue)
    if QUIT_REQUESTED:
        return 0

    page_size = prompt_page_size(config.default_page_size)
    if QUIT_REQUESTED:
        return 0

    print()
    print(config.describe(queue_name, page_size))
    print()

    try:
        connection = SolaceConnection.connect(config.to_connection_config(queue_name))
    except Exception as exc:
        print(f"Failed to connect to {config.host_url}: {exc}", file=sys.stderr)
        return 1

    file_logger: Optional[EventFileLogger] = None
    if config.log_to_file:
        try:
            file_logger = EventFileLogger.open(config.log_file)
            print(f"log file ready for page dumps (`d`): {file_logger.path}")
        except Exception as exc:
            print(
                f"Could not open log file '{config.log_file}': {exc}. Continuing with console only.",
                file=sys.stderr,
            )

    print("connected.")

    browser = QueueBrowser(connection.service, queue_name)
    try:
        total_count, current_page_messages, current_page = rescan(browser, queue_name, page_size)
    except Exception as exc:
        print(f"Failed to browse queue '{queue_name}': {exc}", file=sys.stderr)
        if file_logger:
            file_logger.close()
        connection.disconnect()
        return 1

    while not QUIT_REQUESTED:
        print("================================================")
        render_page_info(queue_name, current_page, page_size, total_count)
        print()
        render_page_items(current_page_messages, current_page, page_size)
        print()
        print_help()

        try:
            command = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if QUIT_REQUESTED or command == "":
            if QUIT_REQUESTED:
                break
            continue

        action, number = try_parse_command(command)
        if action is None:
            print("  unknown command; enter h for help.")
            continue

        if action == BrowseAction.QUIT:
            QUIT_REQUESTED = True
        elif action == BrowseAction.HELP:
            print_help(detailed=True)
            pause_for_continue()
        elif action == BrowseAction.REFRESH:
            try:
                total_count, current_page_messages, current_page = rescan(
                    browser, queue_name, page_size
                )
            except Exception as exc:
                print(f"Refresh failed: {exc}", file=sys.stderr)
        elif action == BrowseAction.NEXT_PAGE:
            current_page = clamp_page(current_page + 1, total_count, page_size)
            current_page_messages = go_to_page(browser, current_page, page_size, total_count)
        elif action == BrowseAction.PREV_PAGE:
            current_page = clamp_page(current_page - 1, total_count, page_size)
            current_page_messages = go_to_page(browser, current_page, page_size, total_count)
        elif action == BrowseAction.GO_TO_PAGE:
            current_page = clamp_page(number, total_count, page_size)
            current_page_messages = go_to_page(browser, current_page, page_size, total_count)
        elif action == BrowseAction.DUMP_PAGE:
            if dump_page(
                current_page_messages,
                current_page,
                page_size,
                file_logger,
                log_to_file=config.log_to_file,
            ):
                pause_for_continue()
        elif action == BrowseAction.DUMP_ITEM:
            if dump_item(browser, number, total_count):
                pause_for_continue()
        elif action == BrowseAction.CHANGE_PAGE_SIZE:
            changed, new_size = prompt_new_page_size(page_size)
            if changed:
                page_size = new_size
                current_page = 1
                current_page_messages = (
                    go_to_page(browser, 1, page_size, total_count) if total_count > 0 else []
                )

    if file_logger:
        file_logger.close()
    connection.disconnect()
    print()
    print("bye.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
