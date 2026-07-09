"""Render inbound messages for console output and NDJSON logging."""



from __future__ import annotations



import base64

import json

from datetime import datetime, timezone

from typing import Any, Callable, Dict, Optional



from solace.messaging.receiver.message_receiver import InboundMessage



PREVIEW_LENGTH = 200





def _safe(getter: Callable[[], Any], default: Any = None) -> Any:

    try:

        return getter()

    except Exception:

        return default





def _safe_attr(message: InboundMessage, attr: str, default: Any = None) -> Any:

    """Call message.attr() when present; tolerate missing methods on browsed messages."""

    getter = getattr(message, attr, None)

    if getter is None:

        return default

    return _safe(getter, default)





def _format_local_now() -> str:

    now = datetime.now().astimezone()

    ms = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    offset = now.strftime("%z")

    if len(offset) >= 5:

        offset = f"{offset[:3]}:{offset[3:]}"

    return f"{ms} {offset} (local)"





def _format_timestamp_from_ms(timestamp_ms: int) -> str:

    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)

    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"





def _format_timestamp_iso_utc(timestamp_ms: int) -> str:

    dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)

    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"





def _payload_bytes(message: InboundMessage) -> Optional[bytes]:

    raw = _safe_attr(message, "get_payload_as_bytes")

    if raw is None:

        raw = _safe_attr(message, "get_binary_attachment")

    if raw is None:

        return None

    if isinstance(raw, bytearray):

        return bytes(raw)

    if isinstance(raw, bytes):

        return raw

    return None





def _payload_text(payload: Optional[bytes]) -> str:

    if not payload:

        return ""

    return payload.decode("utf-8", errors="replace")





def _preview(payload: Optional[bytes], max_length: int = PREVIEW_LENGTH) -> str:

    if not payload:

        return "<empty>"

    text = _payload_text(payload).replace("\r", " ").replace("\n", " ").replace("\t", " ")

    if len(text) > max_length:

        return text[:max_length] + "..."

    return text





def _message_id(message: InboundMessage) -> tuple[Optional[str], Optional[int]]:

    app_id = _safe_attr(message, "get_application_message_id")

    if app_id:

        return str(app_id), None

    seq = _safe_attr(message, "get_sequence_number")

    if seq and int(seq) > 0:

        return None, int(seq)

    rgm_id = _safe_attr(message, "get_replication_group_message_id")

    if rgm_id is not None:

        return str(rgm_id), None

    return None, None





def _resolve_message_timestamp_ms(message: InboundMessage) -> Optional[int]:

    """Best available message time: sender timestamp, then client receive timestamp."""

    sender_ts = _safe_attr(message, "get_sender_timestamp")

    if sender_ts and int(sender_ts) > 0:

        return int(sender_ts)



    receive_ts = _safe_attr(message, "get_time_stamp")

    if receive_ts and int(receive_ts) > 0:

        return int(receive_ts)



    return None





def _browse_timestamp(message: InboundMessage) -> str:

    ts = _resolve_message_timestamp_ms(message)

    return _format_timestamp_from_ms(ts) if ts else "ts=—"





def _simple_console_timestamp(message: InboundMessage) -> str:

    ts = _resolve_message_timestamp_ms(message)

    if ts:

        return _format_timestamp_from_ms(ts)

    return _format_local_now()





def _json_safe(value: Any) -> Any:

    if value is None:

        return None

    if isinstance(value, (str, int, float, bool)):

        return value

    if isinstance(value, (bytes, bytearray)):

        return base64.b64encode(bytes(value)).decode("ascii")

    if isinstance(value, dict):

        return {str(k): _json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple)):

        return [_json_safe(v) for v in value]

    return str(value)





def _user_properties(message: InboundMessage) -> Optional[Dict[str, Any]]:

    props = _safe_attr(message, "get_properties")

    if not props:

        return None

    result: Dict[str, Any] = {}

    for key, value in props.items():

        if value is None:

            continue

        result[str(key)] = _json_safe(value)

    return result or None





def _metadata_dump(message: InboundMessage) -> str:

    dump = _safe(lambda: str(message))

    if dump:

        marker = "Binary Attachment:"

        idx = dump.find(marker)

        if idx >= 0:

            dump = dump[:idx]

        return dump.strip()

    return "<dump unavailable>"





def format_simple_console(message: InboundMessage, index: int) -> str:

    payload = _payload_bytes(message)

    lines = [

        "",

        f"---- MESSAGE #{index} ----",

        f"  {'timestamp':14}: {_simple_console_timestamp(message)}",

    ]

    dest = _safe_attr(message, "get_destination_name")

    if dest:

        lines.append(f"  {'destination':14}: {dest}")

    corr = _safe_attr(message, "get_correlation_id")

    if corr:

        lines.append(f"  {'correlationId':14}: {corr}")

    app_id, seq = _message_id(message)

    if app_id:

        lines.append(f"  {'appMsgId':14}: {app_id}")

    if seq:

        lines.append(f"  {'adMsgId':14}: {seq}")

    lines.append(

        f"  {'payload':14}: {len(payload) if payload else 0} bytes | {_preview(payload)}"

    )

    return "\n".join(lines) + "\n"





def format_compact_browser_line(message: InboundMessage, absolute_index: int) -> str:

    payload = _payload_bytes(message)

    nbytes = len(payload) if payload else 0

    parts = [f"  #{absolute_index}  ", f"{_browse_timestamp(message)}  "]

    app_id, seq = _message_id(message)

    if app_id:

        parts.append(f"appMsgId={app_id}  ")

    elif seq:

        parts.append(f"adMsgId={seq}  ")

    dest = _safe_attr(message, "get_destination_name")

    if dest:

        parts.append(f"dest={dest}  ")

    parts.append(f"{nbytes}b\n")

    return "".join(parts)





def format_full_console(message: InboundMessage, index: int) -> str:

    payload = _payload_bytes(message)

    lines = [

        "",

        f"================== MESSAGE #{index} ==================",

        f"  received_at (local) : {_format_local_now()}",

        "  ----- METADATA -----",

    ]

    for meta_line in _metadata_dump(message).splitlines():

        lines.append(f"  {meta_line}")

    lines.append(f"  ----- PAYLOAD ({len(payload) if payload else 0} bytes) -----")

    if payload:

        lines.append(_payload_text(payload))

    else:

        lines.append("  <no binary attachment>")

    lines.append("==================================================")

    return "\n".join(lines) + "\n"





def build_json_line(message: InboundMessage) -> str:

    payload = _payload_bytes(message)

    record: Dict[str, Any] = {

        "receivedAt": datetime.now().astimezone().isoformat(),

    }



    dest = _safe_attr(message, "get_destination_name")

    if dest:

        record["destination"] = dest

    app_id = _safe_attr(message, "get_application_message_id")

    if app_id:

        record["applicationMessageId"] = app_id

    app_type = _safe_attr(message, "get_application_message_type")

    if app_type:

        record["applicationMessageType"] = app_type

    corr = _safe_attr(message, "get_correlation_id")

    if corr:

        record["correlationId"] = corr

    sender = _safe_attr(message, "get_sender_id")

    if sender:

        record["senderId"] = sender



    sender_ts = _safe_attr(message, "get_sender_timestamp")

    if sender_ts and int(sender_ts) > 0:

        record["senderTimestamp"] = int(sender_ts)

        record["senderTimestampIso"] = _format_timestamp_iso_utc(int(sender_ts))



    receive_ts = _safe_attr(message, "get_time_stamp")

    if receive_ts and int(receive_ts) > 0:

        record["receiverTimestamp"] = int(receive_ts)

        record["receiverTimestampIso"] = _format_timestamp_iso_utc(int(receive_ts))



    seq = _safe_attr(message, "get_sequence_number")

    if seq and int(seq) > 0:

        record["adMessageId"] = int(seq)



    rgm_id = _safe_attr(message, "get_replication_group_message_id")

    if rgm_id is not None:

        record["replicationGroupMessageId"] = str(rgm_id)



    redelivered = _safe_attr(message, "is_redelivered")

    if redelivered is not None:

        record["redelivered"] = bool(redelivered)



    priority = _safe_attr(message, "get_priority")

    if priority is not None:

        record["priority"] = _json_safe(priority)



    expiration = _safe_attr(message, "get_expiration")

    if expiration and int(expiration) > 0:

        record["expiration"] = int(expiration)



    ttl = _safe_attr(message, "get_time_to_live")

    if ttl and int(ttl) > 0:

        record["timeToLive"] = int(ttl)



    user_props = _user_properties(message)

    if user_props:

        record["userProperties"] = user_props



    record["payloadBytes"] = len(payload) if payload else 0

    record["payload"] = _payload_text(payload) if payload else ""



    return json.dumps(record, ensure_ascii=False, separators=(",", ":"))


