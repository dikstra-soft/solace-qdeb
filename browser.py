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
from common.cli import parse_env_file
from common.config import BrowserAppConfig
from common.connection import SolaceConnection
from common.event_file_logger import EventFileLogger
from common.queue_browser import QueueBrowser

QUIT_REQUESTED = False
DUMP_ALL_ACTIVE = False
DUMP_ALL_CANCEL = False


def _install_quit_handler() -> None:
    def handler(signum, frame):  # noqa: ARG001
        global QUIT_REQUESTED, DUMP_ALL_CANCEL
        if DUMP_ALL_ACTIVE:
            DUMP_ALL_CANCEL = True
            return
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


def render_page_info(
    queue_name: str,
    page: int,
    page_size: int,
    known_total: Optional[int],
    message_count: int,
) -> None:
    if known_total is not None:
        pages = total_pages(known_total, page_size)
        page_label = f"{page}/{pages}"
        if known_total == 0:
            items_label = "none"
        else:
            first = (page - 1) * page_size + 1
            last = min(page * page_size, known_total)
            items_label = f"{first}-{last} of {known_total}"
    else:
        page_label = f"{page}+"
        if message_count == 0:
            items_label = "none"
        else:
            first = (page - 1) * page_size + 1
            items_label = f"{first}-{first + message_count - 1} of ?"
    print()
    print(
        f"queue: {queue_name}  |  page {page_label}  |  "
        f"items {items_label}  |  page size {page_size}"
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
            "  r           refresh (reload page 1, no full count scan)\n"
            "  i<n>        full dump of absolute item #n (1 = oldest, e.g. i5)\n"
            "  d           full dump of current page (console + log file)\n"
            "  a / a<n>    dump all from oldest to file (optional limit, e.g. a100)\n"
            "  h           this help\n"
            "  q           quit\n"
            "Item #1 is the oldest message. Page 1 is the head of the queue (native browse order).\n"
            "Browsing is non-destructive. Queue length is discovered while paging."
        )
    else:
        print("Commands: <n>, n/p, s size, i<N>, d dump page, a/a<N> dump all, r refresh, h help, q quit")


class BrowseAction(str, Enum):
    QUIT = "quit"
    HELP = "help"
    REFRESH = "refresh"
    NEXT_PAGE = "next"
    PREV_PAGE = "prev"
    GO_TO_PAGE = "goto"
    DUMP_PAGE = "dump"
    DUMP_ALL = "dump_all"
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
        "a": BrowseAction.DUMP_ALL,
        "all": BrowseAction.DUMP_ALL,
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

    all_match = re.fullmatch(r"a(\d+)", lower)
    if all_match:
        return BrowseAction.DUMP_ALL, int(all_match.group(1))

    all_prefix = re.fullmatch(r"(?:a|all)\s+(\d+)", command, flags=re.IGNORECASE)
    if all_prefix:
        return BrowseAction.DUMP_ALL, int(all_prefix.group(1))

    if lower.isdigit():
        return BrowseAction.GO_TO_PAGE, int(lower)

    return None, 0


def load_page_1(
    browser: QueueBrowser,
    page_size: int,
    known_total: Optional[int],
) -> Tuple[List[InboundMessage], int, Optional[int]]:
    messages = browser.fetch_page(1, page_size)
    if len(messages) < page_size:
        known_total = len(messages)
    return messages, 1, known_total


def try_go_to_page(
    browser: QueueBrowser,
    page: int,
    page_size: int,
    current_page: int,
    current_messages: List[InboundMessage],
    known_total: Optional[int],
) -> Tuple[List[InboundMessage], int, Optional[int]]:
    if page < 1:
        page = 1
    if known_total is not None:
        page = max(1, min(page, total_pages(known_total, page_size)))
    if page == current_page:
        return current_messages, current_page, known_total

    fetched = browser.fetch_page(page, page_size)
    if len(fetched) == 0 and page > 1:
        if page == current_page + 1 and len(current_messages) == page_size:
            known_total = current_page * page_size
        return current_messages, current_page, known_total

    if len(fetched) < page_size:
        known_total = (page - 1) * page_size + len(fetched)
    return fetched, page, known_total


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
                file_logger.write_event(message)
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


def dump_all(
    browser: QueueBrowser,
    limit: int,
    file_logger: Optional[EventFileLogger],
    *,
    log_to_file: bool,
) -> bool:
    global DUMP_ALL_ACTIVE, DUMP_ALL_CANCEL
    if file_logger is None:
        if log_to_file:
            print("  (log file unavailable — open failed at startup; cannot dump all)")
        else:
            print("  (log file disabled — set SOLACE_LOG_TO_FILE=true to dump all to a file)")
        return False

    cap = limit if limit > 0 else None
    cap_label = f"oldest {cap}" if cap is not None else "all (oldest-first)"
    print(f"  dumping {cap_label} to {file_logger.path} ...")

    logged = 0
    failed = 0
    aborted = False
    DUMP_ALL_CANCEL = False

    def cancelled() -> bool:
        return DUMP_ALL_CANCEL

    DUMP_ALL_ACTIVE = True
    try:
        file_logger.write_line("")
        file_logger.write_line("--- solace-browser dump-all ---")
        try:
            for message in browser.for_each_from_oldest(cap, cancelled=cancelled):
                try:
                    file_logger.write_event(message)
                    logged += 1
                    if logged == 1 or logged % 100 == 0:
                        print(f"\r  dumped {logged}...", end="", flush=True)
                except Exception as exc:
                    failed += 1
                    _ = exc
        except KeyboardInterrupt:
            DUMP_ALL_CANCEL = True
            aborted = True
        file_logger.write_line("--- solace-browser dump-all ---")
        file_logger.write_line("")
    except Exception as exc:
        print()
        print(f"  [logfile] dump-all failed: {exc}", file=sys.stderr)
        return True
    finally:
        DUMP_ALL_ACTIVE = False

    print()
    if DUMP_ALL_CANCEL or aborted:
        extra = f" ({failed} failed)" if failed else ""
        print(f"  dump-all aborted after {logged} item(s){extra}.")
    elif failed:
        print(f"  dumped {logged} item(s) to {file_logger.path} ({failed} failed)")
    else:
        print(f"  dumped {logged} item(s) to {file_logger.path}")
    return True


def dump_item(browser: QueueBrowser, absolute_item: int, known_total: Optional[int]) -> bool:
    if absolute_item < 1:
        print(f"  item #{absolute_item} is out of range.")
        return False
    if known_total is not None and (known_total == 0 or absolute_item > known_total):
        print(f"  item #{absolute_item} is out of range (1..{known_total}).")
        return False
    message = browser.fetch_item(absolute_item)
    if message is None:
        print(f"  item #{absolute_item} is out of range.")
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

    env_file, cli_error = parse_env_file()
    if cli_error:
        print(cli_error, file=sys.stderr)
        return 1

    try:
        config, env_path = BrowserAppConfig.load(env_file)
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        if env_file is None:
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
            file_logger = EventFileLogger.open(config.log_file, config.log_format)
            print(f"log file ready for dumps (`d` page, `a` all): {file_logger.path}")
        except Exception as exc:
            print(
                f"Could not open log file '{config.log_file}': {exc}. Continuing with console only.",
                file=sys.stderr,
            )

    print("connected.")

    browser = QueueBrowser(connection.service, queue_name)
    known_total: Optional[int] = None
    try:
        current_page_messages, current_page, known_total = load_page_1(
            browser, page_size, known_total
        )
    except Exception as exc:
        print(f"Failed to browse queue '{queue_name}': {exc}", file=sys.stderr)
        if file_logger:
            file_logger.close()
        connection.disconnect()
        return 1

    while not QUIT_REQUESTED:
        print("================================================")
        render_page_info(
            queue_name, current_page, page_size, known_total, len(current_page_messages)
        )
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
                known_total = None
                current_page_messages, current_page, known_total = load_page_1(
                    browser, page_size, known_total
                )
            except Exception as exc:
                print(f"Refresh failed: {exc}", file=sys.stderr)
        elif action == BrowseAction.NEXT_PAGE:
            current_page_messages, current_page, known_total = try_go_to_page(
                browser, current_page + 1, page_size, current_page, current_page_messages, known_total
            )
        elif action == BrowseAction.PREV_PAGE:
            current_page_messages, current_page, known_total = try_go_to_page(
                browser, current_page - 1, page_size, current_page, current_page_messages, known_total
            )
        elif action == BrowseAction.GO_TO_PAGE:
            current_page_messages, current_page, known_total = try_go_to_page(
                browser, number, page_size, current_page, current_page_messages, known_total
            )
        elif action == BrowseAction.DUMP_PAGE:
            if dump_page(
                current_page_messages,
                current_page,
                page_size,
                file_logger,
                log_to_file=config.log_to_file,
            ):
                pause_for_continue()
        elif action == BrowseAction.DUMP_ALL:
            if dump_all(browser, number, file_logger, log_to_file=config.log_to_file):
                pause_for_continue()
        elif action == BrowseAction.DUMP_ITEM:
            if dump_item(browser, number, known_total):
                pause_for_continue()
        elif action == BrowseAction.CHANGE_PAGE_SIZE:
            changed, new_size = prompt_new_page_size(page_size)
            if changed:
                page_size = new_size
                current_page_messages, current_page, known_total = load_page_1(
                    browser, page_size, known_total
                )

    if file_logger:
        file_logger.close()
    connection.disconnect()
    print()
    print("bye.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
