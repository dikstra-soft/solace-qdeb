#!/usr/bin/env python3
"""solace-consumer — SMF debug consumer (topic or queue)."""

from __future__ import annotations

import queue
import sys
import threading
from typing import Optional

from solace.messaging.config.solace_properties.receiver_properties import (
    PERSISTENT_MESSAGE_ACK_STRATEGY,
)
from solace.messaging.receiver.message_receiver import InboundMessage, MessageHandler
from solace.messaging.resources.queue import Queue
from solace.messaging.resources.topic_subscription import TopicSubscription

from common import payload_formatter
from common.config import AppConfig, ConsumeMode, SourceType
from common.connection import SolaceConnection
from common.event_file_logger import EventFileLogger
from common.prompts import (
    PromptPhase,
    RuntimeState,
    install_signal_handler,
    prompt_mode,
    prompt_queue,
    prompt_topic,
    read_line_cancellable,
    should_abort_prompt,
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


def _make_handler(sink: MessageSink) -> MessageHandler:
    class Handler(MessageHandler):
        def on_message(self, message: InboundMessage) -> None:
            sink.enqueue(message)

    return Handler()


def _consume_async_loop(
    config: AppConfig,
    mode: str,
    collection: queue.Queue[InboundMessage],
    runtime: RuntimeState,
    receiver,
    file_logger: Optional[EventFileLogger],
    received_counter: list[int],
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
                file_logger.write_line(payload_formatter.build_json_line(message))
            except Exception as exc:
                print(f"[logfile] write failed: {exc}", file=sys.stderr)

        if mode == "step":
            print("[step] press Enter for next message (Ctrl+C to stop)... ", end="", flush=True)
            read_line_cancellable(runtime)
            if runtime.cancel_requested or runtime.quit_requested:
                break

        if config.source == SourceType.QUEUE:
            receiver.ack(message)

        if runtime.cancel_requested or runtime.quit_requested:
            break


def _consume_queue_step_sync(
    config: AppConfig,
    runtime: RuntimeState,
    receiver,
    file_logger: Optional[EventFileLogger],
    received_counter: list[int],
) -> None:
    while not runtime.cancel_requested and not runtime.quit_requested:
        message = receiver.receive_message(timeout=RECEIVE_TIMEOUT_MS)
        if message is None:
            if runtime.cancel_requested or runtime.quit_requested:
                break
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
                file_logger.write_line(payload_formatter.build_json_line(message))
            except Exception as exc:
                print(f"[logfile] write failed: {exc}", file=sys.stderr)

        print("[step] press Enter for next message (Ctrl+C to stop)... ", end="", flush=True)
        read_line_cancellable(runtime)
        if runtime.cancel_requested or runtime.quit_requested:
            break

        receiver.ack(message)


def _drain_queue(collection: queue.Queue[InboundMessage]) -> None:
    while True:
        try:
            collection.get_nowait()
        except queue.Empty:
            break


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    print("solace-consumer  (SMF debug consumer)")
    print("-------------------------------------")

    try:
        config, env_path = AppConfig.load()
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        print("Create a .env file (see .env.example).", file=sys.stderr)
        return 1

    if env_path is None:
        print("config source : process environment (no .env file found)")
    else:
        print(f"config source : {env_path}")
    print(config.describe())
    print()

    runtime = RuntimeState()
    install_signal_handler(runtime)
    sink = MessageSink()
    received_counter = [0]

    try:
        connection = SolaceConnection.connect(config)
    except Exception as exc:
        print(f"Failed to connect to {config.host_url}: {exc}", file=sys.stderr)
        return 1

    file_logger: Optional[EventFileLogger] = None
    if config.log_to_file:
        try:
            file_logger = EventFileLogger.open(config.log_file)
            print(f"logging events (NDJSON) to: {file_logger.path}")
        except Exception as exc:
            print(
                f"Could not open log file '{config.log_file}': {exc}. Continuing with console only.",
                file=sys.stderr,
            )

    print("connected.")

    runtime.phase = PromptPhase.STARTUP
    if config.source == SourceType.QUEUE:
        session_queue = prompt_queue(config.queue, runtime)
        if session_queue is None or runtime.quit_requested:
            if file_logger:
                file_logger.close()
            connection.disconnect()
            return 0
        session_topic = ""
    else:
        session_topic = prompt_topic(config.topic, runtime)
        if session_topic is None or runtime.quit_requested:
            if file_logger:
                file_logger.close()
            connection.disconnect()
            return 0
        session_queue = ""

    while not runtime.quit_requested:
        runtime.clear_cancel()
        runtime.phase = PromptPhase.MENU
        print()
        mode = prompt_mode(config.mode.value, runtime)
        if mode is None or runtime.quit_requested:
            break

        runtime.phase = PromptPhase.START_GATE
        print("press Enter to start consuming (Ctrl+C to stop & return to menu)... ", end="", flush=True)
        read_line_cancellable(runtime)
        if runtime.quit_requested:
            break
        if runtime.cancel_requested:
            runtime.clear_cancel()
            continue

        bounded = config.source == SourceType.TOPIC and mode == "step"
        collection: queue.Queue[InboundMessage] = queue.Queue(maxsize=1 if bounded else 0)
        sink.reset(collection)
        receiver = None

        try:
            if config.source == SourceType.TOPIC:
                receiver = (
                    connection.service.create_direct_message_receiver_builder()
                    .with_subscriptions([TopicSubscription.of(session_topic)])
                    .build()
                )
                receiver.start()
                receiver.receive_async(_make_handler(sink))
            elif mode == "step":
                queue_ref = Queue.durable_non_exclusive_queue(session_queue)
                receiver = (
                    connection.service.create_persistent_message_receiver_builder()
                    .from_properties({PERSISTENT_MESSAGE_ACK_STRATEGY: CLIENT_ACK})
                    .build(queue_ref)
                )
                receiver.start()
            else:
                queue_ref = Queue.durable_non_exclusive_queue(session_queue)
                receiver = (
                    connection.service.create_persistent_message_receiver_builder()
                    .from_properties({PERSISTENT_MESSAGE_ACK_STRATEGY: CLIENT_ACK})
                    .build(queue_ref)
                )
                receiver.start()
                receiver.receive_async(_make_handler(sink))

            runtime.phase = PromptPhase.CONSUMING
            if mode == "step":
                print("step mode: press Enter after each message to consume the next one.")
            else:
                print("continuous mode: streaming messages.")

            if config.source == SourceType.QUEUE and mode == "step":
                _consume_queue_step_sync(config, runtime, receiver, file_logger, received_counter)
            else:
                _consume_async_loop(
                    config, mode, collection, runtime, receiver, file_logger, received_counter
                )
        except Exception as exc:
            print(f"Failed to set up {config.source.value}: {exc}", file=sys.stderr)
        finally:
            sink.reset(None)
            if receiver is not None:
                try:
                    receiver.terminate()
                except Exception:
                    pass
            _drain_queue(collection)
            runtime.clear_cancel()
            runtime.phase = PromptPhase.MENU

        dropped_note = f", dropped={sink.dropped} (paused/topic)" if sink.dropped > 0 else ""
        print()
        print(f"stopped. consumed={received_counter[0]}{dropped_note}")

    if runtime.quit_requested:
        print()
        print("stopping...")

    if file_logger:
        file_logger.close()
    connection.disconnect()

    dropped_note = f", dropped={sink.dropped} (paused/topic)" if sink.dropped > 0 else ""
    print()
    print(f"bye. consumed={received_counter[0]}{dropped_note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
