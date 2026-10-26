from __future__ import annotations

import hashlib
import sys
import time
import uuid
from pathlib import Path

from solace.messaging.messaging_service import MessagingService
from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisher
from solace.messaging.publisher.persistent_message_publisher import PersistentMessagePublisher
from solace.messaging.resources.topic import Topic

from common.config import MessageDeliveryKind, SourceType
from common.prompts import RuntimeState, StdinLinePump, read_line_xfer, should_abort_prompt, throw_if_quit
from common.xfer import envelope, progress
from common.xfer.errors import XferError
from common.xfer.limits import MAX_FILE_BYTES, QUEUE_DEST_PREFIX


def destination_topic(dest_type: SourceType, dest_name: str) -> Topic:
    if dest_type == SourceType.QUEUE:
        return Topic.of(f"{QUEUE_DEST_PREFIX}{dest_name}")
    return Topic.of(dest_name)


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _build_message(
    service: MessagingService,
    transfer_id: str,
    app_msg_id: str,
    props: dict[str, str],
    payload: bytes,
):
    builder = envelope.apply_common_headers(
        service.message_builder(), transfer_id, app_msg_id, props
    )
    body = bytearray(payload) if payload else bytearray()
    return builder.build(body)


def send_one(
    service: MessagingService,
    dest_type: SourceType,
    dest_name: str,
    delivery: MessageDeliveryKind,
    transfer_id: str,
    app_msg_id: str,
    props: dict[str, str],
    payload: bytes,
    persistent: PersistentMessagePublisher,
    direct: DirectMessagePublisher,
    ack_timeout_ms: int,
    runtime: RuntimeState,
) -> None:
    throw_if_quit(runtime)
    outbound = _build_message(service, transfer_id, app_msg_id, props, payload)
    dest = destination_topic(dest_type, dest_name)
    try:
        if delivery == MessageDeliveryKind.PERSISTENT:
            persistent.publish_await_acknowledgement(outbound, dest, ack_timeout_ms)
        else:
            direct.publish(outbound, dest)
    except Exception as exc:
        raise XferError(f"send failed: {exc}") from exc


def run(
    service: MessagingService,
    dest_type: SourceType,
    dest_name: str,
    file_path: str,
    chunk_bytes: int,
    runtime: RuntimeState,
    stdin: StdinLinePump,
    persistent: PersistentMessagePublisher,
    direct: DirectMessagePublisher,
    ack_timeout_ms: int,
) -> None:
    path = Path(file_path)
    if not path.is_file():
        raise XferError(f"file not found: {file_path}")

    size = path.stat().st_size
    if size > MAX_FILE_BYTES:
        raise XferError(
            f"file is {progress.format_bytes(size)} ({size} bytes); "
            f"max is {progress.format_bytes(MAX_FILE_BYTES)}"
        )

    print(f"file       : {path.resolve()}")
    print(f"size       : {size} bytes ({progress.format_bytes(size)})")
    print(f"chunk size : {chunk_bytes} bytes")
    print("computing SHA-256...", end="", flush=True)
    sha256 = hash_file(path)
    print(f" {sha256}")

    print("Has the receiver been started? [y]/n: ", end="", flush=True)
    confirm = read_line_xfer(runtime, stdin)
    if should_abort_prompt(runtime, confirm):
        raise KeyboardInterrupt
    if (confirm or "").strip().lower() in ("n", "no"):
        raise XferError("aborted: receiver not started")

    delivery = (
        MessageDeliveryKind.PERSISTENT
        if dest_type == SourceType.QUEUE
        else MessageDeliveryKind.DIRECT
    )
    if dest_type == SourceType.TOPIC:
        print(file=sys.stderr)
        print(
            f"WARN: publishing Direct to topic '{dest_name}'. "
            "The receiver MUST already be subscribed or this transfer will be lost.",
            file=sys.stderr,
        )

    total_chunks = 0 if size == 0 else int((size + chunk_bytes - 1) // chunk_bytes)
    transfer_id = uuid.uuid4().hex
    meta = envelope.MetaPayload(
        kind=envelope.KIND_META,
        transfer_id=transfer_id,
        file_name=path.name,
        file_size=size,
        chunk_size=chunk_bytes,
        total_chunks=total_chunks,
        sha256=sha256,
    )

    started = time.perf_counter()
    send_one(
        service,
        dest_type,
        dest_name,
        delivery,
        transfer_id,
        f"{transfer_id}:meta",
        {envelope.PROP_KIND: envelope.KIND_META, envelope.PROP_ID: transfer_id},
        meta.to_json().encode("utf-8"),
        persistent,
        direct,
        ack_timeout_ms,
        runtime,
    )
    print(
        f"sent META  transferId={transfer_id}  file={meta.file_name}  "
        f"size={meta.file_size}  chunks={total_chunks}  sha256={sha256}"
    )

    sent_bytes = 0
    with path.open("rb") as handle:
        for seq in range(total_chunks):
            throw_if_quit(runtime)
            payload = handle.read(chunk_bytes)
            if not payload:
                raise XferError("unexpected end of file while reading chunks")
            send_one(
                service,
                dest_type,
                dest_name,
                delivery,
                transfer_id,
                f"{transfer_id}:data:{seq}",
                {
                    envelope.PROP_KIND: envelope.KIND_DATA,
                    envelope.PROP_ID: transfer_id,
                    envelope.PROP_SEQ: str(seq),
                },
                payload,
                persistent,
                direct,
                ack_timeout_ms,
                runtime,
            )
            sent_bytes += len(payload)
            progress.write(sent_bytes, size, seq + 1, total_chunks)

    if total_chunks > 0:
        progress.finish()

    end = envelope.EndPayload(
        kind=envelope.KIND_END, transfer_id=transfer_id, sha256=sha256
    )
    send_one(
        service,
        dest_type,
        dest_name,
        delivery,
        transfer_id,
        f"{transfer_id}:end",
        {envelope.PROP_KIND: envelope.KIND_END, envelope.PROP_ID: transfer_id},
        end.to_json().encode("utf-8"),
        persistent,
        direct,
        ack_timeout_ms,
        runtime,
    )
    elapsed = time.perf_counter() - started
    print(f"sent END   sha256={sha256}")
    print()
    print("---- SEND OK ----")
    print(f"  path     : {path.resolve()}")
    print(f"  size     : {size} bytes ({progress.format_bytes(size)})")
    print(f"  sha256   : {sha256}")
    print(f"  duration : {elapsed:.3f}s")
    print(f"  dest     : {dest_type.value} '{dest_name}'")
