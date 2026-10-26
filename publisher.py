#!/usr/bin/env python3
"""Interactive SMF debug publisher (topic or direct-to-queue)."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from solace.messaging.config.solace_properties import message_properties
from solace.messaging.messaging_service import MessagingService
from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisher
from solace.messaging.publisher.persistent_message_publisher import PersistentMessagePublisher
from solace.messaging.resources.queue import Queue
from solace.messaging.resources.topic import Topic

from common.cli import parse_env_file
from common.config import (
    MessageDeliveryKind,
    PublisherAppConfig,
    SourceType,
)
from common.connection import SolaceConnection
from common.prompts import (
    PromptPhase,
    RuntimeState,
    StdinLinePump,
    install_publisher_signal_handler,
    prompt_queue,
    prompt_topic,
    read_line_cancellable,
    should_abort_prompt,
)

# Python PubSub+ publishers only accept Topic destinations. Direct-to-queue
# uses the broker reserved topic #P2P/QUE/<queueName> (same semantics as
# setting an IQueue destination in the .NET API).
_QUEUE_DEST_PREFIX = "#P2P/QUE/"


class PublishAction(str, Enum):
    QUIT = "quit"
    HELP = "help"
    SEND = "send"
    SEND_N = "send_n"
    SEND_RAW = "send_raw"
    RESEND = "resend"
    CHANGE_DESTINATION = "cd"


@dataclass
class SessionStats:
    attempts: int = 0
    success: int = 0
    failed: int = 0
    total_payload_bytes: int = 0
    started_at: Optional[datetime] = None

    def mark_started(self) -> None:
        if self.started_at is None:
            self.started_at = datetime.now().astimezone()


@dataclass
class LastSend:
    payload: bytes
    dest_type: SourceType
    dest_name: str
    delivery: MessageDeliveryKind
    dest_from_json: bool = False
    application_message_id: Optional[str] = None
    correlation_id: Optional[str] = None
    application_message_type: Optional[str] = None
    sender_id: Optional[str] = None
    http_content_type: Optional[str] = None
    priority: Optional[int] = None
    time_to_live: Optional[int] = None
    expiration: Optional[int] = None
    user_properties: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(
        cls,
        payload: bytes,
        dest_type: SourceType,
        dest_name: str,
        delivery: MessageDeliveryKind,
    ) -> "LastSend":
        return cls(
            payload=payload,
            dest_type=dest_type,
            dest_name=dest_name,
            delivery=delivery,
            dest_from_json=False,
        )


@dataclass
class PublisherHandles:
    direct: DirectMessagePublisher
    persistent: PersistentMessagePublisher

    def terminate(self) -> None:
        for pub in (self.direct, self.persistent):
            try:
                if not pub.is_terminated:
                    pub.terminate(grace_period=0)
            except Exception:
                pass


def quit_with_stats(stats: SessionStats) -> int:
    print()
    print("session stats:")
    print(f"  attempts = {stats.attempts}")
    print(f"  success  = {stats.success}")
    print(f"  failed   = {stats.failed}")
    print(f"  total_payload_bytes = {stats.total_payload_bytes}")
    if stats.started_at is not None:
        duration = datetime.now().astimezone() - stats.started_at
        print(f"  duration = {duration.total_seconds():.3f}s")
    print()
    print("bye.")
    return 0


def print_help(*, detailed: bool = False) -> None:
    if detailed:
        print(
            """
Commands:
  s           send payload (prompt on next line; sent as-is UTF-8 bytes)
  sr          send raw event from one NDJSON/JSON line (consumer log style)
  s<n>        send the same payload n times (e.g. s5)
  r           resend last payload
  cd          change destination (topic/queue name and mode)
  h           this help
  q           quit (print session stats)

sr restores ApplicationMessageId, CorrelationId, ApplicationMessageType,
SenderId, UserPropertyMap, DeliveryMode, Priority, TTL, and payload when
present. Broker-only fields (adMessageId, receiverTimestamp,
replicationGroupMessageId, redelivered) cannot be set on outbound
messages and are skipped with a WARN.
Destination: JSON "destination" if present, else the current menu dest.
""".rstrip()
        )
    else:
        print(
            "Commands: s send, sr raw NDJSON, s<n> send n, r resend, "
            "cd change dest, h help, q quit"
        )


def pause_for_continue(runtime: RuntimeState, stdin: StdinLinePump) -> None:
    if runtime.quit_requested:
        return
    print("press Enter to continue... ", end="", flush=True)
    read_line_cancellable(runtime, stdin)


def try_parse_command(command: str) -> Tuple[Optional[PublishAction], int]:
    lower = command.lower()
    if lower in ("q", "quit", "exit"):
        return PublishAction.QUIT, 1
    if lower in ("h", "help", "?"):
        return PublishAction.HELP, 1
    if lower in ("r", "resend"):
        return PublishAction.RESEND, 1
    if lower == "sr":
        return PublishAction.SEND_RAW, 1
    if lower == "cd":
        return PublishAction.CHANGE_DESTINATION, 1
    if lower in ("s", "send"):
        return PublishAction.SEND, 1
    if len(lower) > 1 and lower[0] == "s":
        try:
            count = int(lower[1:])
        except ValueError:
            return None, 1
        if count > 0:
            return PublishAction.SEND_N, count
    return None, 1


def destination_topic(dest_type: SourceType, dest_name: str) -> Topic:
    if dest_type == SourceType.QUEUE:
        return Topic.of(f"{_QUEUE_DEST_PREFIX}{dest_name}")
    return Topic.of(dest_name)


def try_validate_queue(service: MessagingService, queue_name: str) -> bool:
    browser = None
    try:
        queue = Queue.durable_non_exclusive_queue(queue_name)
        browser = (
            service.create_message_queue_browser_builder()
            .with_queue_browser_window_size(1)
            .build(queue)
        )
        browser.start()
        return True
    except Exception as exc:
        print(
            f"ERROR: cannot bind/browse queue '{queue_name}': {exc}",
            file=sys.stderr,
        )
        return False
    finally:
        if browser is not None:
            try:
                browser.terminate()
            except Exception:
                pass


def try_change_destination(
    service: MessagingService,
    config: PublisherAppConfig,
    dest_type: SourceType,
    dest_name: str,
    delivery: MessageDeliveryKind,
    runtime: RuntimeState,
    stdin: StdinLinePump,
) -> Tuple[bool, SourceType, str, MessageDeliveryKind]:
    prev_type, prev_name, prev_delivery = dest_type, dest_name, delivery

    while not runtime.quit_requested:
        print(
            f"Type? [t]opic / [q]ueue (current {dest_type.value}): ",
            end="",
            flush=True,
        )
        type_input = read_line_cancellable(runtime, stdin)
        if should_abort_prompt(runtime, type_input):
            return False, dest_type, dest_name, delivery
        choice = (type_input or "").strip().lower()

        if choice == "":
            new_type = dest_type
        elif choice in ("t", "topic"):
            new_type = SourceType.TOPIC
        elif choice in ("q", "queue"):
            new_type = SourceType.QUEUE
        else:
            print("  invalid choice; enter t or q.")
            continue

        default_name = (
            dest_name
            if new_type == dest_type
            else (config.queue if new_type == SourceType.QUEUE else config.topic)
        )
        if new_type == SourceType.QUEUE:
            chosen = prompt_queue(default_name, runtime, stdin)
            if chosen is None or runtime.quit_requested:
                return False, dest_type, dest_name, delivery
            dest = chosen
        else:
            topics = prompt_topic(default_name, runtime, stdin)
            if topics is None or runtime.quit_requested:
                return False, dest_type, dest_name, delivery
            dest = topics[0]

        if new_type == SourceType.QUEUE and not try_validate_queue(service, dest):
            print("ERROR: keeping previous destination.", file=sys.stderr)
            return False, prev_type, prev_name, prev_delivery

        new_delivery = config.resolve_delivery(new_type)
        print(
            f"destination set to {new_type.value} '{dest}' ({new_delivery.value})."
        )
        return True, new_type, dest, new_delivery

    return False, dest_type, dest_name, delivery


def build_outbound(service: MessagingService, blueprint: LastSend):
    builder = service.message_builder()
    if blueprint.application_message_id:
        builder = builder.with_application_message_id(blueprint.application_message_id)
    if blueprint.correlation_id:
        builder = builder.with_correlation_id(blueprint.correlation_id)
    if blueprint.application_message_type:
        builder = builder.with_application_message_type(
            blueprint.application_message_type
        )
    if blueprint.sender_id:
        builder = builder.with_sender_id(blueprint.sender_id)
    if blueprint.priority is not None:
        builder = builder.with_priority(blueprint.priority)
    if blueprint.expiration is not None and blueprint.expiration > 0:
        builder = builder.with_expiration(blueprint.expiration)
    if blueprint.http_content_type:
        builder = builder.with_http_content_header(blueprint.http_content_type, "")

    props: Dict[str, Any] = {}
    if blueprint.time_to_live is not None and blueprint.time_to_live > 0:
        props[message_properties.PERSISTENT_TIME_TO_LIVE] = int(blueprint.time_to_live)

    for key, value in blueprint.user_properties.items():
        try:
            if isinstance(value, (str, int, float, bool, type(None), bytearray, bytes)):
                if isinstance(value, bytes):
                    builder = builder.with_property(key, bytearray(value))
                else:
                    builder = builder.with_property(key, value)
            elif isinstance(value, (dict, list, tuple)):
                builder = builder.with_property(key, value)
            else:
                builder = builder.with_property(key, str(value))
        except Exception as exc:
            print(f"WARN: skipping userProperty '{key}': {exc}", file=sys.stderr)

    if props:
        builder = builder.from_properties(props)

    payload = blueprint.payload
    if isinstance(payload, bytes) and not isinstance(payload, bytearray):
        return builder.build(bytearray(payload))
    return builder.build(payload if payload else bytearray())


def print_send_feedback(
    blueprint: LastSend,
    return_code: str,
    success: bool,
    sent_at: datetime,
    index: int,
    total: int,
) -> None:
    prefix = f"[{index}/{total}] " if total > 1 else ""
    status = "OK" if success else "FAIL"
    dest_note = " (from JSON)" if blueprint.dest_from_json else " (current)"
    offset = sent_at.strftime("%z")
    if len(offset) >= 5:
        offset = f"{offset[:3]}:{offset[3:]}"
    print()
    print(f"---- SEND {prefix}{status} ----")
    print(f"  sentAt      : {sent_at.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} {offset}")
    print(f"  returnCode  : {return_code}")
    print(
        f"  destination : {blueprint.dest_type.value} '{blueprint.dest_name}'{dest_note}"
    )
    print(f"  delivery    : {blueprint.delivery.value}")
    print(f"  payload     : {len(blueprint.payload)} bytes")
    if blueprint.application_message_id:
        print(f"  appMsgId    : {blueprint.application_message_id}")
    if blueprint.correlation_id:
        print(f"  correlation : {blueprint.correlation_id}")


def send_one(
    service: MessagingService,
    handles: PublisherHandles,
    blueprint: LastSend,
    stats: SessionStats,
    index: int,
    total: int,
    ack_timeout_ms: int,
) -> bool:
    stats.mark_started()
    stats.attempts += 1
    sent_at = datetime.now().astimezone()
    try:
        outbound = build_outbound(service, blueprint)
        dest = destination_topic(blueprint.dest_type, blueprint.dest_name)
        if blueprint.delivery == MessageDeliveryKind.PERSISTENT:
            handles.persistent.publish_await_acknowledgement(
                outbound, dest, ack_timeout_ms
            )
            return_code = "OK (acked)"
        else:
            handles.direct.publish(outbound, dest)
            return_code = "OK"
        stats.success += 1
        stats.total_payload_bytes += len(blueprint.payload)
        print_send_feedback(blueprint, return_code, True, sent_at, index, total)
        return True
    except Exception as exc:
        stats.failed += 1
        print(f"ERROR: send failed: {exc}", file=sys.stderr)
        print_send_feedback(blueprint, f"FAIL ({exc})", False, sent_at, index, total)
        return False


def send_copies(
    service: MessagingService,
    handles: PublisherHandles,
    blueprint: LastSend,
    count: int,
    stats: SessionStats,
    runtime: RuntimeState,
    ack_timeout_ms: int,
) -> None:
    ok = 0
    for i in range(count):
        if runtime.quit_requested:
            break
        if send_one(service, handles, blueprint, stats, i + 1, count, ack_timeout_ms):
            ok += 1
    if count > 1:
        print(f"  summary: {ok}/{count} succeeded")


def _warn_if_present(root: dict, name: str, warnings: List[str]) -> None:
    if name in root:
        warnings.append(name)


def _get_string(root: dict, name: str) -> Optional[str]:
    value = root.get(name)
    if not isinstance(value, str) or value == "":
        return None
    return value


def _get_int(root: dict, name: str) -> Optional[int]:
    value = root.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def try_build_raw_send(
    json_line: str,
    current_type: SourceType,
    current_name: str,
    current_delivery: MessageDeliveryKind,
) -> Tuple[Optional[LastSend], str]:
    try:
        root = json.loads(json_line)
    except json.JSONDecodeError as exc:
        return None, f"invalid JSON: {exc}"

    if not isinstance(root, dict):
        return None, "JSON must be an object."

    warnings: List[str] = []
    for field_name in (
        "adMessageId",
        "receiverTimestamp",
        "receiverTimestampIso",
        "replicationGroupMessageId",
        "redelivered",
        "receivedAt",
        "senderTimestamp",
        "senderTimestampIso",
    ):
        _warn_if_present(root, field_name, warnings)

    if warnings:
        print(
            "WARN: cannot restore broker-only / receive-only fields on outbound messages:",
            file=sys.stderr,
        )
        for name in warnings:
            print(f"  - {name}", file=sys.stderr)

    json_dest = root.get("destination")
    dest_from_json = isinstance(json_dest, str) and bool(json_dest.strip())
    dest_name = json_dest.strip() if dest_from_json else current_name
    dest_type = current_type

    delivery = current_delivery
    dm = root.get("deliveryMode")
    if isinstance(dm, str):
        normalized = dm.strip().lower()
        if normalized in ("persistent", "guaranteed"):
            delivery = MessageDeliveryKind.PERSISTENT
        elif normalized == "direct":
            delivery = MessageDeliveryKind.DIRECT

    payload_text = root.get("payload")
    if isinstance(payload_text, str):
        payload = payload_text.encode("utf-8")
    else:
        payload = b""

    user_props: Dict[str, Any] = {}
    up = root.get("userProperties")
    if isinstance(up, dict):
        user_props = dict(up)

    return (
        LastSend(
            payload=payload,
            dest_type=dest_type,
            dest_name=dest_name,
            dest_from_json=dest_from_json,
            delivery=delivery,
            application_message_id=_get_string(root, "applicationMessageId"),
            correlation_id=_get_string(root, "correlationId"),
            application_message_type=_get_string(root, "applicationMessageType"),
            sender_id=_get_string(root, "senderId"),
            http_content_type=_get_string(root, "httpContentType"),
            priority=_get_int(root, "priority"),
            time_to_live=_get_int(root, "timeToLive"),
            expiration=_get_int(root, "expiration"),
            user_properties=user_props,
        ),
        "",
    )


def start_publishers(service: MessagingService) -> PublisherHandles:
    direct = service.create_direct_message_publisher_builder().build()
    direct.start()
    persistent = service.create_persistent_message_publisher_builder().build()
    persistent.start()
    return PublisherHandles(direct=direct, persistent=persistent)


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    print("solace-publisher  (SMF debug publisher)")
    print("--------------------------------------")

    env_file, cli_error = parse_env_file()
    if cli_error:
        print(cli_error, file=sys.stderr)
        return 1

    try:
        config, env_path = PublisherAppConfig.load(env_file)
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        if env_file is None:
            print("Create a .env file (see .env.example).", file=sys.stderr)
        return 1

    if env_path is None:
        print("config source : process environment (no .env file found)")
    else:
        print(f"config source : {env_path}")
    print()

    runtime = RuntimeState()
    stats = SessionStats()
    last_send: Optional[LastSend] = None
    stdin = StdinLinePump()
    install_publisher_signal_handler(runtime)

    try:
        connection = SolaceConnection.connect(config.to_connection_config())
    except Exception as exc:
        print(f"Failed to connect to {config.host_url}: {exc}", file=sys.stderr)
        stdin.close()
        return 1

    print("connected.")
    handles: Optional[PublisherHandles] = None

    try:
        handles = start_publishers(connection.service)
        ack_timeout_ms = max(config.connect_timeout_ms, 10000)

        runtime.phase = PromptPhase.STARTUP
        dest_type = config.source
        if dest_type == SourceType.QUEUE:
            chosen = prompt_queue(config.queue, runtime, stdin)
            if chosen is None or runtime.quit_requested:
                return quit_with_stats(stats)
            dest_name = chosen
            if not try_validate_queue(connection.service, dest_name):
                return 1
        else:
            topics = prompt_topic(config.topic, runtime, stdin)
            if topics is None or runtime.quit_requested:
                return quit_with_stats(stats)
            dest_name = topics[0]

        delivery = config.resolve_delivery(dest_type)
        print()
        print(config.describe(dest_type, dest_name, delivery))

        while not runtime.quit_requested:
            runtime.clear_cancel()
            runtime.phase = PromptPhase.MENU
            print()
            print_help()
            print(f"> [{dest_type.value}:{dest_name}] ", end="", flush=True)
            raw = read_line_cancellable(runtime, stdin)
            if should_abort_prompt(runtime, raw) or runtime.quit_requested:
                break

            command = (raw or "").strip()
            if not command:
                continue

            action, count = try_parse_command(command)
            if action is None:
                print("  unknown command; enter h for help.")
                continue

            if action == PublishAction.QUIT:
                runtime.request_quit()
                break

            if action == PublishAction.HELP:
                print_help(detailed=True)
                pause_for_continue(runtime, stdin)
                continue

            if action in (PublishAction.SEND, PublishAction.SEND_N):
                runtime.phase = PromptPhase.PAYLOAD
                print("payload> ", end="", flush=True)
                payload_text = read_line_cancellable(runtime, stdin)
                if should_abort_prompt(runtime, payload_text) or runtime.quit_requested:
                    break
                payload = (payload_text or "").encode("utf-8")
                last_send = LastSend.from_payload(
                    payload, dest_type, dest_name, delivery
                )
                n = count if action == PublishAction.SEND_N else 1
                send_copies(
                    connection.service,
                    handles,
                    last_send,
                    n,
                    stats,
                    runtime,
                    ack_timeout_ms,
                )
                continue

            if action == PublishAction.SEND_RAW:
                runtime.phase = PromptPhase.PAYLOAD
                print("ndjson> ", end="", flush=True)
                line = read_line_cancellable(runtime, stdin)
                if should_abort_prompt(runtime, line) or runtime.quit_requested:
                    break
                if not (line or "").strip():
                    print("ERROR: empty input; nothing to send.", file=sys.stderr)
                    continue
                raw_send, error = try_build_raw_send(
                    line or "", dest_type, dest_name, delivery
                )
                if raw_send is None:
                    print(f"ERROR: {error}", file=sys.stderr)
                    continue
                last_send = raw_send
                send_copies(
                    connection.service,
                    handles,
                    last_send,
                    1,
                    stats,
                    runtime,
                    ack_timeout_ms,
                )
                continue

            if action == PublishAction.RESEND:
                if last_send is None:
                    print(
                        "WARN: nothing sent yet; send with s / sr / s<n> first.",
                        file=sys.stderr,
                    )
                    continue
                send_copies(
                    connection.service,
                    handles,
                    last_send,
                    1,
                    stats,
                    runtime,
                    ack_timeout_ms,
                )
                continue

            if action == PublishAction.CHANGE_DESTINATION:
                runtime.phase = PromptPhase.PAYLOAD
                ok, dest_type, dest_name, delivery = try_change_destination(
                    connection.service,
                    config,
                    dest_type,
                    dest_name,
                    delivery,
                    runtime,
                    stdin,
                )
                if runtime.quit_requested:
                    break
                if ok:
                    print(config.describe(dest_type, dest_name, delivery))
                continue

        return quit_with_stats(stats)
    finally:
        if handles is not None:
            handles.terminate()
        connection.disconnect()
        stdin.close()


if __name__ == "__main__":
    raise SystemExit(main())
