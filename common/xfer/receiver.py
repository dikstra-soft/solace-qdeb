from __future__ import annotations

import hashlib
import sys
import time
from pathlib import Path
from typing import Optional

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic_subscription import TopicSubscription

from common.config import SourceType
from common.prompts import (
    RuntimeState,
    StdinLinePump,
    read_line_xfer,
    should_abort_prompt,
    throw_if_quit,
)
from common.xfer import envelope, progress, queue_ops
from common.xfer.errors import XferError
from common.xfer.limits import PEEK_TIMEOUT_MS


def run(
    service: MessagingService,
    dest_type: SourceType,
    dest_name: str,
    created_by_us: bool,
    dest_path_hint: Optional[str],
    runtime: RuntimeState,
    stdin: StdinLinePump,
) -> None:
    if dest_type == SourceType.QUEUE:
        _run_queue(service, dest_name, created_by_us, dest_path_hint, runtime, stdin)
    else:
        _run_topic(service, dest_name, dest_path_hint, runtime, stdin)


def _peek_meta(service: MessagingService, queue_name: str, runtime: RuntimeState):
    browser = queue_ops.peek_browser(service, queue_name)
    try:
        browser.start()
        while True:
            throw_if_quit(runtime)
            message = browser.receive_message(timeout=PEEK_TIMEOUT_MS)
            if message is None:
                continue
            meta, err = envelope.try_parse_meta(message)
            if meta is None:
                return None, err
            return meta, None
    finally:
        try:
            browser.terminate()
        except Exception:
            pass


def _fail_first(reason: str, created_by_us: bool) -> None:
    print(f"ERROR: first message is not META ({reason}). Queue left untouched.", file=sys.stderr)
    if not created_by_us:
        print(
            "Not acking / not consuming. The sender may need to reset, or the queue owner should delete the queue.",
            file=sys.stderr,
        )
    raise XferError(f"first message is not META ({reason})")


def _run_queue(
    service: MessagingService,
    queue_name: str,
    created_by_us: bool,
    dest_path_hint: Optional[str],
    runtime: RuntimeState,
    stdin: StdinLinePump,
) -> None:
    print(f"waiting for META on queue '{queue_name}' (browser peek, non-destructive)...")
    peeked = None
    peek_error = None
    browser_worked = False
    try:
        peeked, peek_error = _peek_meta(service, queue_name, runtime)
        browser_worked = peek_error is None and peeked is not None
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        print(
            f"WARN: browser peek failed ({exc}); falling back to consume-and-hold (ClientAck, no NACK).",
            file=sys.stderr,
        )

    if browser_worked:
        dest_path = _prompt_dest_path(peeked, dest_path_hint, runtime, stdin)
        _consume_after_peek(service, queue_name, peeked, dest_path, runtime)
        return

    if peek_error is not None:
        _fail_first(peek_error, created_by_us)
        return

    print("holding first message unacked until filename is confirmed (will not NACK/Reject).")
    _consume_hold_unacked(service, queue_name, created_by_us, dest_path_hint, runtime, stdin)


def _take(receiver, runtime: RuntimeState):
    while True:
        throw_if_quit(runtime)
        message = receiver.receive_message(timeout=200)
        if message is not None:
            return message


def _consume_after_peek(
    service: MessagingService,
    queue_name: str,
    expected: envelope.MetaPayload,
    dest_path: str,
    runtime: RuntimeState,
) -> None:
    receiver = queue_ops.build_hold_receiver(service, queue_name)
    try:
        receiver.start()
        first = _take(receiver, runtime)
        meta, err = envelope.try_parse_meta(first)
        if meta is None or meta.transfer_id != expected.transfer_id:
            raise XferError(
                f"flow first message did not match peeked META ({err}). Not acking."
            )
        receiver.ack(first)
        _drain_rest(receiver, expected, dest_path, runtime, ack=True)
    finally:
        try:
            receiver.terminate()
        except Exception:
            pass


def _consume_hold_unacked(
    service: MessagingService,
    queue_name: str,
    created_by_us: bool,
    dest_path_hint: Optional[str],
    runtime: RuntimeState,
    stdin: StdinLinePump,
) -> None:
    receiver = queue_ops.build_hold_receiver(service, queue_name)
    try:
        receiver.start()
        print(f"waiting for META on queue '{queue_name}' (ClientAck hold, WindowSize 1)...")
        first = _take(receiver, runtime)
        meta, err = envelope.try_parse_meta(first)
        if meta is None:
            print(
                f"ERROR: first message is not META ({err}). Not acking (no NACK/Reject).",
                file=sys.stderr,
            )
            if not created_by_us:
                print(
                    "Message stays on the queue. The sender may need to reset, or the queue owner should delete the queue.",
                    file=sys.stderr,
                )
            raise XferError(f"first message is not META ({err})")
        dest_path = _prompt_dest_path(meta, dest_path_hint, runtime, stdin)
        receiver.ack(first)
        _drain_rest(receiver, meta, dest_path, runtime, ack=True)
    finally:
        try:
            receiver.terminate()
        except Exception:
            pass


def _drain_rest(receiver, meta: envelope.MetaPayload, dest_path: str, runtime: RuntimeState, *, ack: bool) -> None:
    started = time.perf_counter()
    hasher = hashlib.sha256()
    written = 0
    complete = False
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    handle = dest.open("wb")
    try:
        for seq in range(meta.total_chunks):
            msg = _take(receiver, runtime)
            kind = envelope.read_kind(msg)
            if kind != envelope.KIND_DATA:
                raise XferError(
                    f"expected DATA seq={seq}, got kind '{kind or '(none)'}'. Not acking this message."
                    if ack
                    else f"expected DATA seq={seq}, got kind '{kind or '(none)'}'"
                )
            ident = envelope.get_transfer_id(msg)
            if ident != meta.transfer_id:
                raise XferError(
                    f"DATA transferId mismatch (got '{ident}'). Not acking."
                    if ack
                    else f"DATA transferId mismatch (got '{ident}')"
                )
            ok, got_seq = envelope.try_get_seq(msg)
            if not ok or got_seq != seq:
                raise XferError(
                    f"out-of-order DATA (expected seq={seq}, got {got_seq}). Not acking."
                    if ack
                    else f"out-of-order DATA (expected seq={seq}, got {got_seq})"
                )
            payload = envelope.binary(msg)
            handle.write(payload)
            hasher.update(payload)
            written += len(payload)
            if ack:
                receiver.ack(msg)
            progress.write(written, meta.file_size, seq + 1, meta.total_chunks)

        if meta.total_chunks > 0:
            progress.finish()

        end_msg = _take(receiver, runtime)
        end, err = envelope.try_parse_end(end_msg)
        if end is None:
            raise XferError(f"{err}. Not acking." if ack else err)
        if end.transfer_id != meta.transfer_id:
            raise XferError(
                f"END transferId mismatch (got '{end.transfer_id}'). Not acking."
                if ack
                else f"END transferId mismatch (got '{end.transfer_id}')"
            )
        if ack:
            receiver.ack(end_msg)

        handle.flush()
        complete = True
        elapsed = time.perf_counter() - started
        actual = hasher.hexdigest()
        match = actual.lower() == meta.sha256.lower()
        print()
        print("---- RECV OK ----" if match else "---- RECV HASH MISMATCH ----")
        print(f"  path     : {dest_path}")
        print(f"  size     : {written} bytes ({progress.format_bytes(written)}) (meta {meta.file_size})")
        print(f"  sha256   : {actual}")
        print(f"  expected : {meta.sha256}")
        print(f"  duration : {elapsed:.3f}s")
        if not match:
            raise XferError("SHA-256 mismatch")
    finally:
        try:
            handle.close()
        except Exception:
            pass
        if not complete:
            _try_delete_partial(dest_path)


def _run_topic(
    service: MessagingService,
    topic_name: str,
    dest_path_hint: Optional[str],
    runtime: RuntimeState,
    stdin: StdinLinePump,
) -> None:
    subscriptions = [TopicSubscription.of(topic_name)]
    receiver = (
        service.create_direct_message_receiver_builder()
        .with_subscriptions(subscriptions)
        .build()
    )
    try:
        receiver.start()
        print(f"subscribed to topic '{topic_name}' (Direct). Waiting for META...")
        print(
            "WARN: Direct/topic has no queue. If META is missed, the transfer is lost.",
            file=sys.stderr,
        )
        first = _take(receiver, runtime)
        meta, err = envelope.try_parse_meta(first)
        if meta is None:
            raise XferError(
                f"first message is not META ({err}). Direct message already delivered (cannot unread)."
            )
        dest_path = _prompt_dest_path(meta, dest_path_hint, runtime, stdin)
        _drain_rest(receiver, meta, dest_path, runtime, ack=False)
    finally:
        try:
            receiver.terminate()
        except Exception:
            pass


def _prompt_dest_path(
    meta: envelope.MetaPayload,
    dest_path_hint: Optional[str],
    runtime: RuntimeState,
    stdin: StdinLinePump,
) -> str:
    suggested = Path(dest_path_hint) if dest_path_hint else Path(Path(meta.file_name).name)
    suggested = suggested.resolve()
    print(
        f"META  transferId={meta.transfer_id}  file={meta.file_name}  "
        f"size={meta.file_size}  chunks={meta.total_chunks}"
    )
    print(f"Save as? (default {suggested}): ", end="", flush=True)
    value = read_line_xfer(runtime, stdin)
    if should_abort_prompt(runtime, value):
        raise KeyboardInterrupt
    chosen = suggested if not (value or "").strip() else Path(value.strip()).resolve()
    return str(_avoid_overwrite(chosen, runtime, stdin))


def _avoid_overwrite(path: Path, runtime: RuntimeState, stdin: StdinLinePump) -> Path:
    if not path.is_file():
        return path
    alt = _next_free_path(path)
    print(
        f"File '{path}' exists (will not overwrite). Use '{alt}' instead? [y]/n: ",
        end="",
        flush=True,
    )
    ans = read_line_xfer(runtime, stdin)
    if should_abort_prompt(runtime, ans):
        raise KeyboardInterrupt
    if (ans or "").strip().lower() in ("n", "no"):
        raise XferError("aborted: destination file exists")
    return _next_free_path(path)


def _next_free_path(path: Path) -> Path:
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    for n in range(1, 10_000):
        candidate = parent / f"{stem}-{n}{suffix}"
        if not candidate.exists():
            return candidate
    raise XferError(f"could not find a free name for '{path}'")


def _try_delete_partial(path: str) -> None:
    try:
        target = Path(path)
        if target.is_file():
            target.unlink()
            print(f"removed incomplete file '{path}'", file=sys.stderr)
    except Exception as exc:
        print(f"WARN: could not remove incomplete file '{path}': {exc}", file=sys.stderr)
