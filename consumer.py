#!/usr/bin/env python3
"""solace-consumer — SMF debug consumer (topic or queue)."""

from __future__ import annotations

import queue
import sys
import threading
from pathlib import Path
from typing import Optional

from solace.messaging.config.solace_properties.receiver_properties import (
    PERSISTENT_MESSAGE_ACK_STRATEGY,
)
from solace.messaging.messaging_service import ServiceEvent
from solace.messaging.receiver.message_receiver import InboundMessage, MessageHandler
from solace.messaging.resources.queue import Queue
from solace.messaging.resources.topic_subscription import TopicSubscription

from common import payload_formatter
from common.cli import parse_env_file
from common.config import AppConfig, LogFormat, ReconnectMode, SourceType
from common.connection import SolaceConnection
from common.event_file_logger import EventFileLogger, resolve_log_path
from common.prompts import (
    PromptPhase,
    RuntimeState,
    StdinLinePump,
    install_signal_handler,
    prompt_mode,
    prompt_queue,
    prompt_source_type,
    prompt_topic,
    read_line_cancellable,
)

try:
    from solace.messaging.config.solace_constants import (
        persistent_receiver_acknowledgement_constants as ack_constants,
    )

    CLIENT_ACK = ack_constants.PERSISTENT_RECEIVER_CLIENT_ACK
except Exception:  # pragma: no cover - fallback for API constant layout changes
    CLIENT_ACK = "PERSISTENT_RECEIVER_CLIENT_ACK"

RECEIVE_TIMEOUT_MS = 1000


class MessageSink:
    def __init__(self) -> None:
        self._target: Optional[queue.Queue[InboundMessage]] = None
        self.dropped = 0
        self._lock = threading.Lock()

    def reset(self, target: Optional[queue.Queue[InboundMessage]]) -> None:
        with self._lock:
            self._target = target

    def enqueue(self, message: InboundMessage) -> None:
        with self._lock:
            target = self._target
        if target is None:
            return
        try:
            target.put_nowait(message)
        except queue.Full:
            self.dropped += 1


class ActiveConsume:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.active = False
        self.source: SourceType = SourceType.TOPIC
        self.mode = "continuous"
        self.topics: list[str] = []
        self.queue_name = ""
        self.receiver = None
        self.collection: Optional[queue.Queue[InboundMessage]] = None
        self.rebind_generation = 0

    def begin(
        self,
        source: SourceType,
        mode: str,
        topics: list[str],
        queue_name: str,
        collection: Optional[queue.Queue[InboundMessage]],
    ) -> None:
        with self.lock:
            self.source = source
            self.mode = mode
            self.topics = list(topics)
            self.queue_name = queue_name
            self.collection = collection
            self.receiver = None
            self.active = True
            self.rebind_generation += 1

    def end(self) -> None:
        with self.lock:
            self.active = False
            self.receiver = None
            self.collection = None


def _make_handler(sink: MessageSink) -> MessageHandler:
    class Handler(MessageHandler):
        def on_message(self, message: InboundMessage) -> None:
            sink.enqueue(message)

    return Handler()


def format_bytes(num: int) -> str:
    if num >= 1024 * 1024:
        return f"{num / (1024 * 1024):.2f} MB"
    if num >= 1024:
        return f"{num / 1024:.2f} KB"
    return f"{num} B"


def print_consume_summary(
    source: SourceType,
    mode: str,
    topics: list[str],
    queue_name: str,
    reconnect: ReconnectMode,
) -> None:
    if source == SourceType.TOPIC:
        if len(topics) <= 1:
            dest = topics[0] if topics else ""
        else:
            dest = f"topics={len(topics)} [{', '.join(topics)}]"
    else:
        dest = queue_name
    print(
        f"consuming: mode={mode} source={source.value} dest={dest} reconnect={reconnect.value}",
        flush=True,
    )


def _build_receiver(
    connection: SolaceConnection,
    source: SourceType,
    mode: str,
    topics: list[str],
    queue_name: str,
    sink: MessageSink,
    collection: Optional[queue.Queue[InboundMessage]],
):
    if source == SourceType.TOPIC:
        subscriptions = [TopicSubscription.of(name) for name in topics]
        receiver = (
            connection.service.create_direct_message_receiver_builder()
            .with_subscriptions(subscriptions)
            .build()
        )
        receiver.start()
        if collection is not None:
            sink.reset(collection)
        receiver.receive_async(_make_handler(sink))
        return receiver

    queue_ref = Queue.durable_non_exclusive_queue(queue_name)
    receiver = (
        connection.service.create_persistent_message_receiver_builder()
        .from_properties({PERSISTENT_MESSAGE_ACK_STRATEGY: CLIENT_ACK})
        .build(queue_ref)
    )
    receiver.start()
    if mode != "step":
        if collection is not None:
            sink.reset(collection)
        receiver.receive_async(_make_handler(sink))
    return receiver


def _terminate_receiver(receiver) -> None:
    if receiver is None:
        return
    try:
        receiver.terminate()
    except Exception:
        pass


def _request_consume_stop(runtime: RuntimeState, reason: str) -> None:
    runtime.set_stop_reason(reason)
    runtime.cancel_active()


def _rebind_after_reconnect(
    connection: SolaceConnection,
    consume: ActiveConsume,
    sink: MessageSink,
    config: AppConfig,
) -> None:
    with consume.lock:
        if not consume.active:
            return
        old = consume.receiver
        consume.receiver = None
        _terminate_receiver(old)
        sink.reset(None)
        collection = consume.collection
        if consume.source == SourceType.TOPIC or consume.mode != "step":
            # async path needs a live collection
            if collection is None:
                collection = queue.Queue()
                consume.collection = collection
        receiver = _build_receiver(
            connection,
            consume.source,
            consume.mode,
            consume.topics,
            consume.queue_name,
            sink,
            collection,
        )
        consume.receiver = receiver
        consume.rebind_generation += 1

    print("[reconnect] session reconnected; rebinding consume...", file=sys.stderr, flush=True)
    print_consume_summary(
        consume.source,
        consume.mode,
        consume.topics,
        consume.queue_name,
        config.reconnect_mode,
    )


def _consume_async_loop(
    config: AppConfig,
    mode: str,
    collection: queue.Queue[InboundMessage],
    runtime: RuntimeState,
    consume: ActiveConsume,
    file_logger: Optional[EventFileLogger],
    received_counter: list[int],
    stdin: StdinLinePump,
) -> None:
    while not runtime.cancel_requested and not runtime.quit_requested:
        try:
            message = collection.get(timeout=0.1)
        except queue.Empty:
            continue

        if runtime.cancel_requested or runtime.quit_requested:
            break

        received_counter[0] += 1
        n = received_counter[0]
        text = (
            payload_formatter.format_full_console(message, n)
            if config.console_full
            else payload_formatter.format_simple_console(message, n)
        )
        print(text, end="", flush=True)

        if file_logger is not None:
            try:
                file_logger.write_event(message)
            except Exception as exc:
                print(f"[logfile] write failed: {exc}", file=sys.stderr)

        if mode == "step":
            print("[step] press Enter for next message (Ctrl+C to stop)... ", end="", flush=True)
            read_line_cancellable(runtime, stdin)
            if runtime.cancel_requested or runtime.quit_requested:
                break

        if consume.source == SourceType.QUEUE:
            with consume.lock:
                receiver = consume.receiver
            if receiver is not None:
                try:
                    receiver.ack(message)
                except Exception as exc:
                    print(f"[ack] {exc}", file=sys.stderr)

        if runtime.cancel_requested or runtime.quit_requested:
            break


def _consume_queue_step_sync(
    config: AppConfig,
    runtime: RuntimeState,
    consume: ActiveConsume,
    file_logger: Optional[EventFileLogger],
    received_counter: list[int],
    stdin: StdinLinePump,
) -> None:
    while not runtime.cancel_requested and not runtime.quit_requested:
        with consume.lock:
            receiver = consume.receiver
            generation = consume.rebind_generation
        if receiver is None:
            # Waiting for rebind after reconnect
            if runtime.cancel_requested or runtime.quit_requested:
                break
            threading.Event().wait(0.05)
            continue

        try:
            message = receiver.receive_message(timeout=RECEIVE_TIMEOUT_MS)
        except Exception as exc:
            # Receiver may be dead after disconnect; wait for rebind or stop.
            if runtime.cancel_requested or runtime.quit_requested:
                break
            print(f"[receive] {exc}", file=sys.stderr)
            threading.Event().wait(0.2)
            continue

        if message is None:
            if runtime.cancel_requested or runtime.quit_requested:
                break
            # If rebind happened, loop and pick up new receiver.
            with consume.lock:
                if consume.rebind_generation != generation:
                    continue
            continue

        received_counter[0] += 1
        n = received_counter[0]
        text = (
            payload_formatter.format_full_console(message, n)
            if config.console_full
            else payload_formatter.format_simple_console(message, n)
        )
        print(text, end="", flush=True)

        if file_logger is not None:
            try:
                file_logger.write_event(message)
            except Exception as exc:
                print(f"[logfile] write failed: {exc}", file=sys.stderr)

        print("[step] press Enter for next message (Ctrl+C to stop)... ", end="", flush=True)
        read_line_cancellable(runtime, stdin)
        if runtime.cancel_requested or runtime.quit_requested:
            break

        with consume.lock:
            ack_receiver = consume.receiver
        if ack_receiver is not None:
            try:
                ack_receiver.ack(message)
            except Exception as exc:
                print(f"[ack] {exc}", file=sys.stderr)


def _drain_queue(collection: queue.Queue[InboundMessage]) -> None:
    while True:
        try:
            collection.get_nowait()
        except queue.Empty:
            break


def _event_info(event: ServiceEvent) -> str:
    try:
        return event.get_message()
    except Exception:
        return str(event)


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    print("solace-consumer  (SMF debug consumer)")
    print("-------------------------------------")

    env_file, cli_error = parse_env_file()
    if cli_error:
        print(cli_error, file=sys.stderr)
        return 1

    try:
        config, env_path = AppConfig.load(env_file)
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

    if config.log_to_file and config.log_warn_bytes > 0:
        try:
            log_path = Path(resolve_log_path(config.log_file))
            if log_path.is_file():
                size = log_path.stat().st_size
                if size >= config.log_warn_bytes:
                    print(
                        f"WARN: events log is large: {log_path} "
                        f"({format_bytes(size)}; threshold {format_bytes(config.log_warn_bytes)}).",
                        file=sys.stderr,
                    )
                    print("continue? [y]/n (default y): ", end="", flush=True)
                    answer = (sys.stdin.readline() or "").strip().lower()
                    if answer in ("n", "no"):
                        print("exiting so you can clean up the log file.")
                        return 0
        except Exception as exc:
            print(f"Could not check log file size: {exc}", file=sys.stderr)

    stdin = StdinLinePump()
    runtime = RuntimeState()
    install_signal_handler(runtime)
    sink = MessageSink()
    consume = ActiveConsume()
    received_counter = [0]

    try:
        connection = SolaceConnection.connect(config)
    except Exception as exc:
        print(f"Failed to connect to {config.host_url}: {exc}", file=sys.stderr)
        stdin.close()
        return 1

    def on_reconnecting(event: ServiceEvent) -> None:
        info = _event_info(event)
        print(f"[session] reconnecting: {info}", file=sys.stderr, flush=True)
        if runtime.phase == PromptPhase.CONSUMING and consume.active:
            if config.reconnect_mode == ReconnectMode.STOP:
                _request_consume_stop(runtime, "session reconnecting")

    def on_reconnected(event: ServiceEvent) -> None:
        info = _event_info(event)
        print(f"[session] reconnected: {info}", file=sys.stderr, flush=True)
        if runtime.phase != PromptPhase.CONSUMING or not consume.active:
            return
        if config.reconnect_mode == ReconnectMode.STOP:
            _request_consume_stop(runtime, "session reconnected")
            return
        try:
            _rebind_after_reconnect(connection, consume, sink, config)
        except Exception as exc:
            print(f"[reconnect] rebind failed: {exc}", file=sys.stderr)
            _request_consume_stop(runtime, "rebind failed after reconnect")

    def on_interrupted(event: ServiceEvent) -> None:
        info = _event_info(event)
        print(f"[session] interrupted: {info}", file=sys.stderr, flush=True)
        if runtime.phase == PromptPhase.CONSUMING and consume.active:
            _request_consume_stop(runtime, "session interrupted")

    connection.set_reconnect_handlers(
        on_reconnecting=on_reconnecting,
        on_reconnected=on_reconnected,
        on_interrupted=on_interrupted,
    )

    file_logger: Optional[EventFileLogger] = None
    if config.log_to_file:
        try:
            file_logger = EventFileLogger.open(config.log_file, config.log_format)
            if config.log_format == LogFormat.SIMPLE:
                fmt_label = "simple"
            elif config.log_format == LogFormat.PAYLOAD_ONLY:
                fmt_label = "payload_only"
            else:
                fmt_label = "NDJSON"
            print(f"logging events ({fmt_label}) to: {file_logger.path}")
        except Exception as exc:
            print(
                f"Could not open log file '{config.log_file}': {exc}. Continuing with console only.",
                file=sys.stderr,
            )

    print("connected.")

    runtime.phase = PromptPhase.STARTUP
    session_source = prompt_source_type(config.source, runtime, stdin)
    if session_source is None or runtime.quit_requested:
        if file_logger:
            file_logger.close()
        connection.disconnect()
        stdin.close()
        return 0

    if session_source == SourceType.QUEUE:
        session_queue = prompt_queue(config.queue, runtime, stdin)
        if session_queue is None or runtime.quit_requested:
            if file_logger:
                file_logger.close()
            connection.disconnect()
            stdin.close()
            return 0
        session_topics: list[str] = []
    else:
        session_topics = prompt_topic(config.topic, runtime, stdin)
        if session_topics is None or runtime.quit_requested:
            if file_logger:
                file_logger.close()
            connection.disconnect()
            stdin.close()
            return 0
        session_queue = ""

    default_mode = config.resolve_mode(session_source).value

    while not runtime.quit_requested:
        runtime.clear_cancel()
        runtime.clear_stop_reason()
        runtime.phase = PromptPhase.MENU
        stdin.drain()
        print()
        mode = prompt_mode(default_mode, runtime, stdin)
        if mode is None or runtime.quit_requested:
            break

        runtime.phase = PromptPhase.START_GATE
        print("press Enter to start consuming (Ctrl+C to stop & return to menu)... ", end="", flush=True)
        read_line_cancellable(runtime, stdin)
        if runtime.quit_requested:
            break
        if runtime.cancel_requested:
            runtime.clear_cancel()
            continue

        bounded = session_source == SourceType.TOPIC and mode == "step"
        collection: queue.Queue[InboundMessage] = queue.Queue(maxsize=1 if bounded else 0)
        sink.reset(collection)
        consume.begin(session_source, mode, session_topics, session_queue, collection)

        try:
            receiver = _build_receiver(
                connection,
                session_source,
                mode,
                session_topics,
                session_queue,
                sink,
                collection,
            )
            with consume.lock:
                consume.receiver = receiver

            runtime.phase = PromptPhase.CONSUMING
            if mode == "step":
                print("step mode: press Enter after each message to consume the next one.")
            else:
                print("continuous mode: streaming messages.")
            print_consume_summary(
                session_source, mode, session_topics, session_queue, config.reconnect_mode
            )

            if session_source == SourceType.QUEUE and mode == "step":
                _consume_queue_step_sync(
                    config, runtime, consume, file_logger, received_counter, stdin
                )
            else:
                _consume_async_loop(
                    config,
                    mode,
                    collection,
                    runtime,
                    consume,
                    file_logger,
                    received_counter,
                    stdin,
                )
        except Exception as exc:
            print(f"Failed to set up {session_source.value}: {exc}", file=sys.stderr)
        finally:
            stop_reason = runtime.consume_stop_reason
            sink.reset(None)
            with consume.lock:
                receiver = consume.receiver
                consume.receiver = None
            _terminate_receiver(receiver)
            consume.end()
            _drain_queue(collection)
            runtime.clear_cancel()
            runtime.phase = PromptPhase.MENU
            stdin.drain()

        dropped_note = f", dropped={sink.dropped} (paused/topic)" if sink.dropped > 0 else ""
        print()
        if stop_reason:
            print(f"stopped ({stop_reason}). consumed={received_counter[0]}{dropped_note}")
        else:
            print(f"stopped. consumed={received_counter[0]}{dropped_note}")

    if runtime.quit_requested:
        print()
        print("stopping...")

    if file_logger:
        file_logger.close()
    connection.disconnect()
    stdin.close()

    dropped_note = f", dropped={sink.dropped} (paused/topic)" if sink.dropped > 0 else ""
    print()
    print(f"bye. consumed={received_counter[0]}{dropped_note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
