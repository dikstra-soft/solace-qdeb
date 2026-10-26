#!/usr/bin/env python3
"""solace-xfer — SMF file transfer (sender / receiver)."""

from __future__ import annotations

import sys
import time
import uuid
from typing import Optional

from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisher
from solace.messaging.publisher.persistent_message_publisher import PersistentMessagePublisher

from common.cli import parse_cli
from common.config import SourceType, XferAppConfig
from common.connection import SolaceConnection
from common.prompts import (
    RuntimeState,
    StdinLinePump,
    install_xfer_signal_handler,
    prompt_yes_no,
    read_line_xfer,
    should_abort_prompt,
    wait_ms,
)
from common.xfer import queue_ops, receiver, sender
from common.xfer.errors import XferError
from common.xfer.limits import EMPTY_POLL_MS, EMPTY_WAIT_SECONDS


def prompt_role(runtime: RuntimeState, stdin: StdinLinePump) -> Optional[str]:
    while not runtime.quit_requested:
        print("Role? [s]ender / [r]eceiver: ", end="", flush=True)
        value = read_line_xfer(runtime, stdin)
        if should_abort_prompt(runtime, value):
            return None
        choice = (value or "").strip().lower()
        if choice in ("s", "sender"):
            return "sender"
        if choice in ("r", "receiver"):
            return "receiver"
        print("  invalid choice; enter s or r.")
    return None


def prompt_dest_type(runtime: RuntimeState, stdin: StdinLinePump) -> Optional[SourceType]:
    while not runtime.quit_requested:
        print("Dest type? [q]ueue / [t]opic (default queue): ", end="", flush=True)
        value = read_line_xfer(runtime, stdin)
        if should_abort_prompt(runtime, value):
            return None
        choice = (value or "").strip().lower()
        if choice in ("", "q", "queue"):
            return SourceType.QUEUE
        if choice in ("t", "topic"):
            return SourceType.TOPIC
        print("  invalid choice; enter q or t.")
    return None


def prompt_dest_name(
    dest_type: SourceType, runtime: RuntimeState, stdin: StdinLinePump
) -> Optional[str]:
    label = "Queue" if dest_type == SourceType.QUEUE else "Topic"
    empty_hint = (
        "empty = generate xfer-{guid8}"
        if dest_type == SourceType.QUEUE
        else "empty = generate xfer/{guid}"
    )
    while not runtime.quit_requested:
        print(f"{label}? ({empty_hint}): ", end="", flush=True)
        value = read_line_xfer(runtime, stdin)
        if should_abort_prompt(runtime, value):
            return None
        name = (value or "").strip()
        if name:
            return name
        if dest_type == SourceType.QUEUE:
            return "xfer-" + uuid.uuid4().hex[:8]
        return "xfer/" + uuid.uuid4().hex
    return None


def prompt_file_path(
    cli_path: Optional[str], runtime: RuntimeState, stdin: StdinLinePump
) -> Optional[str]:
    if cli_path and cli_path.strip():
        return cli_path.strip()
    while not runtime.quit_requested:
        print("File? ", end="", flush=True)
        value = read_line_xfer(runtime, stdin)
        if should_abort_prompt(runtime, value):
            return None
        if (value or "").strip():
            return value.strip()
        print("  path is required.")
    return None


def prompt_chunk_size(
    default_bytes: int, runtime: RuntimeState, stdin: StdinLinePump
) -> Optional[int]:
    while not runtime.quit_requested:
        print(f"Chunk size (bytes)? [{default_bytes}]: ", end="", flush=True)
        value = read_line_xfer(runtime, stdin)
        if should_abort_prompt(runtime, value):
            return None
        if not (value or "").strip():
            return default_bytes
        try:
            n = int(value.strip())
        except ValueError:
            n = 0
        if n < 1 or n > XferAppConfig.MAX_CHUNK_BYTES:
            print(
                f"  must be an integer 1..{XferAppConfig.MAX_CHUNK_BYTES} (Solace common max payload)."
            )
            continue
        return n
    return None


def offer_delete_if_needed(
    success: bool,
    is_sender: bool,
    dest_type: SourceType,
    dest_name: str,
    created_by_us: bool,
    connection: Optional[SolaceConnection],
    runtime: RuntimeState,
    stdin: StdinLinePump,
) -> None:
    if not created_by_us or dest_type != SourceType.QUEUE or connection is None:
        return
    if not dest_name:
        return

    stdin.drain()
    if not success:
        print(f"queue '{dest_name}' may still have messages.", file=sys.stderr)

    wait_empty = success and is_sender
    if wait_empty:
        print(f"waiting up to {EMPTY_WAIT_SECONDS}s for queue '{dest_name}' to empty...")
        deadline = time.monotonic() + EMPTY_WAIT_SECONDS
        while time.monotonic() < deadline and not runtime.quit_requested and not runtime.skip_delete:
            if queue_ops.is_empty(connection.service, dest_name):
                print("queue is empty.")
                break
            wait_ms(EMPTY_POLL_MS, runtime)

    runtime.begin_delete_prompt()
    if runtime.skip_delete:
        print(f"leaving queue '{dest_name}' (clean up later).")
        return

    delete = prompt_yes_no(
        f"Delete queue '{dest_name}'?", True, runtime, stdin, ignore_quit=True
    )
    if runtime.skip_delete or delete is None:
        print(f"leaving queue '{dest_name}' (clean up later).")
        return
    if delete is not True:
        print(f"leaving queue '{dest_name}'.")
        return

    try:
        queue_ops.deprovision(connection.service, dest_name)
        print(f"deleted queue '{dest_name}'.")
    except Exception as exc:
        print(f"ERROR: failed to delete queue '{dest_name}': {exc}", file=sys.stderr)
        print(f"clean up queue '{dest_name}' manually.", file=sys.stderr)


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    print("solace-xfer  (SMF file transfer)")
    print("--------------------------------")

    env_file, remaining, cli_error = parse_cli()
    if cli_error:
        print(cli_error, file=sys.stderr)
        return 1

    try:
        config, env_path = XferAppConfig.load(env_file)
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        if env_file is None:
            print("Create a .env file (see .env.example).", file=sys.stderr)
        return 1

    if env_path is None:
        print("config source : process environment (no .env file found)")
    else:
        print(f"config source : {env_path}")
    print(config.describe())
    print()

    cli_path = remaining[0] if remaining else None
    if len(remaining) > 1:
        print(f"WARN: ignoring extra arguments: {' '.join(remaining[1:])}", file=sys.stderr)

    runtime = RuntimeState()
    stdin = StdinLinePump()
    install_xfer_signal_handler(runtime)

    connection: Optional[SolaceConnection] = None
    dest_type = SourceType.QUEUE
    dest_name = ""
    created_by_us = False
    transfer_ok = False
    role: Optional[str] = None
    handles_direct: Optional[DirectMessagePublisher] = None
    handles_persistent: Optional[PersistentMessagePublisher] = None

    def after_transfer(exit_code: int) -> int:
        offer_delete_if_needed(
            transfer_ok,
            role == "sender",
            dest_type,
            dest_name,
            created_by_us,
            connection,
            runtime,
            stdin,
        )
        return exit_code

    try:
        try:
            connection = SolaceConnection.connect(config.to_connection_config())
        except Exception as exc:
            print(f"Failed to connect to {config.host_url}: {exc}", file=sys.stderr)
            return 1

        print("connected.")
        print()

        role = prompt_role(runtime, stdin)
        if role is None or runtime.quit_requested:
            return 0

        dest = prompt_dest_type(runtime, stdin)
        if dest is None or runtime.quit_requested:
            return 0
        dest_type = dest

        name = prompt_dest_name(dest_type, runtime, stdin)
        if name is None or runtime.quit_requested:
            return 0
        dest_name = name
        print(f"using {dest_type.value}: {dest_name}")

        if dest_type == SourceType.QUEUE:
            ensured = queue_ops.ensure_exclusive_queue(connection.service, dest_name)
            if not ensured.ok:
                print(f"ERROR: {ensured.error}", file=sys.stderr)
                if role == "sender" and ensured.missing_and_cannot_create:
                    fallback = prompt_yes_no(
                        "Publish to a topic instead?", True, runtime, stdin
                    )
                    if fallback is not True:
                        return 1
                    dest_type = SourceType.TOPIC
                    topic_name = prompt_dest_name(SourceType.TOPIC, runtime, stdin)
                    if topic_name is None or runtime.quit_requested:
                        return 1
                    dest_name = topic_name
                    created_by_us = False
                    print(f"using topic: {dest_name}")
                else:
                    if role == "receiver" and ensured.missing_and_cannot_create:
                        print(
                            "Receiver cannot bind a queue that does not exist.",
                            file=sys.stderr,
                        )
                    return 1
            else:
                created_by_us = ensured.created_by_us
                print(
                    f"provisioned exclusive queue '{dest_name}' (we own delete)."
                    if created_by_us
                    else f"queue '{dest_name}' already exists; using it (will not offer delete)."
                )

        if dest_type == SourceType.TOPIC and role == "sender":
            print(file=sys.stderr)
            print(
                f"WARN: delivery is Direct to topic '{dest_name}'. "
                "Start the receiver first and keep it subscribed.",
                file=sys.stderr,
            )

        if role == "sender":
            file_path = prompt_file_path(cli_path, runtime, stdin)
            if file_path is None or runtime.quit_requested:
                return after_transfer(1 if created_by_us and dest_type == SourceType.QUEUE else (0 if runtime.quit_requested else 1))

            chunk = prompt_chunk_size(config.chunk_bytes, runtime, stdin)
            if chunk is None or runtime.quit_requested:
                return after_transfer(1 if created_by_us and dest_type == SourceType.QUEUE else (0 if runtime.quit_requested else 1))

            handles_direct = connection.service.create_direct_message_publisher_builder().build()
            handles_direct.start()
            handles_persistent = connection.service.create_persistent_message_publisher_builder().build()
            handles_persistent.start()
            ack_timeout_ms = max(config.connect_timeout_ms, 10000)
            sender.run(
                connection.service,
                dest_type,
                dest_name,
                file_path,
                chunk,
                runtime,
                stdin,
                handles_persistent,
                handles_direct,
                ack_timeout_ms,
            )
            transfer_ok = True
        else:
            receiver.run(
                connection.service,
                dest_type,
                dest_name,
                created_by_us,
                cli_path,
                runtime,
                stdin,
            )
            transfer_ok = True

        return after_transfer(0)
    except KeyboardInterrupt:
        print()
        print("interrupted.")
        return after_transfer(1)
    except XferError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return after_transfer(1)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return after_transfer(1)
    finally:
        for pub in (handles_direct, handles_persistent):
            if pub is not None:
                try:
                    if not pub.is_terminated:
                        pub.terminate(grace_period=0)
                except Exception:
                    pass
        stdin.close()
        if connection is not None:
            connection.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
