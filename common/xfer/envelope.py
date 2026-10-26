from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from common.config import XferAppConfig
from common.xfer.errors import XferError
from common.xfer.limits import MAX_FILE_BYTES

KIND_META = "meta"
KIND_DATA = "data"
KIND_END = "end"

PROP_KIND = "xfer-kind"
PROP_ID = "xfer-id"
PROP_SEQ = "xfer-seq"


@dataclass
class MetaPayload:
    kind: str
    transfer_id: str
    file_name: str
    file_size: int
    chunk_size: int
    total_chunks: int
    sha256: str
    content_type: Optional[str] = None

    def to_json(self) -> str:
        data: dict[str, Any] = {
            "kind": self.kind,
            "transferId": self.transfer_id,
            "fileName": self.file_name,
            "fileSize": self.file_size,
            "chunkSize": self.chunk_size,
            "totalChunks": self.total_chunks,
            "sha256": self.sha256,
        }
        if self.content_type:
            data["contentType"] = self.content_type
        return json.dumps(data, separators=(",", ":"))


@dataclass
class EndPayload:
    kind: str
    transfer_id: str
    sha256: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "kind": self.kind,
                "transferId": self.transfer_id,
                "sha256": self.sha256,
            },
            separators=(",", ":"),
        )


def _payload_bytes(message) -> bytes:
    raw = None
    for name in ("get_payload_as_bytes", "get_binary_attachment"):
        getter = getattr(message, name, None)
        if getter is None:
            continue
        try:
            raw = getter()
            if raw is not None:
                break
        except Exception:
            continue
    if raw is None:
        return b""
    if isinstance(raw, bytearray):
        return bytes(raw)
    if isinstance(raw, bytes):
        return raw
    return b""


def _user_props(message) -> dict:
    getter = getattr(message, "get_properties", None)
    if getter is None:
        return {}
    try:
        props = getter() or {}
    except Exception:
        return {}
    return props if isinstance(props, dict) else {}


def _user_str(message, key: str) -> Optional[str]:
    props = _user_props(message)
    value = props.get(key)
    if value is None:
        return None
    return str(value)


def _try_json(message) -> Optional[dict]:
    raw = _payload_bytes(message)
    if not raw:
        return None
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def read_kind(message) -> Optional[str]:
    kind = _user_str(message, PROP_KIND)
    if kind:
        return kind.strip().lower()
    data = _try_json(message)
    if data is None:
        return None
    value = data.get("kind")
    if isinstance(value, str):
        return value.strip().lower()
    return None


def get_transfer_id(message) -> Optional[str]:
    ident = _user_str(message, PROP_ID)
    if ident:
        return ident
    data = _try_json(message)
    if data is None:
        return None
    value = data.get("transferId")
    return value if isinstance(value, str) else None


def try_get_seq(message) -> Tuple[bool, int]:
    raw = _user_str(message, PROP_SEQ)
    if raw is not None:
        try:
            return True, int(raw)
        except ValueError:
            pass
    props = _user_props(message)
    value = props.get(PROP_SEQ)
    if isinstance(value, int) and not isinstance(value, bool):
        return True, value
    return False, -1


def binary(message) -> bytes:
    return _payload_bytes(message)


def try_parse_meta(message) -> Tuple[Optional[MetaPayload], str]:
    kind = read_kind(message)
    if kind != KIND_META:
        return None, f"first message kind is '{kind or '(none)'}', expected '{KIND_META}'"

    data = _try_json(message)
    if data is None:
        return None, "META payload is not valid JSON"

    transfer_id = str(data.get("transferId") or "")
    file_name = str(data.get("fileName") or "")
    try:
        file_size = int(data.get("fileSize"))
        chunk_size = int(data.get("chunkSize"))
        total_chunks = int(data.get("totalChunks"))
    except (TypeError, ValueError):
        return None, "META JSON parse failed: numeric fields invalid"
    sha256 = str(data.get("sha256") or "")
    content_type = data.get("contentType")
    if content_type is not None:
        content_type = str(content_type)

    if not transfer_id.strip():
        return None, "META missing transferId"
    if not file_name.strip():
        return None, "META missing fileName"
    if file_size < 0 or file_size > MAX_FILE_BYTES:
        return None, f"META fileSize {file_size} is out of range (max {MAX_FILE_BYTES})"
    if chunk_size < 1 or chunk_size > XferAppConfig.MAX_CHUNK_BYTES:
        return None, (
            f"META chunkSize {chunk_size} is out of range (max {XferAppConfig.MAX_CHUNK_BYTES})"
        )
    if total_chunks < 0:
        return None, "META totalChunks is negative"
    if not sha256.strip():
        return None, "META missing sha256"

    id_prop = _user_str(message, PROP_ID)
    if id_prop and id_prop != transfer_id:
        return None, "META user prop xfer-id does not match JSON transferId"

    return (
        MetaPayload(
            kind=KIND_META,
            transfer_id=transfer_id,
            file_name=file_name,
            file_size=file_size,
            chunk_size=chunk_size,
            total_chunks=total_chunks,
            sha256=sha256,
            content_type=content_type,
        ),
        "",
    )


def try_parse_end(message) -> Tuple[Optional[EndPayload], str]:
    kind = read_kind(message)
    if kind != KIND_END:
        return None, f"expected END, got kind '{kind or '(none)'}'"

    data = _try_json(message)
    if data is None:
        return None, "END payload is not valid JSON"

    transfer_id = str(data.get("transferId") or "")
    sha256 = str(data.get("sha256") or "")
    if not transfer_id.strip():
        return None, "END missing transferId"
    if not sha256.strip():
        return None, "END missing sha256"

    return EndPayload(kind=KIND_END, transfer_id=transfer_id, sha256=sha256), ""


def apply_common_headers(builder, transfer_id: str, app_msg_id: str, props: dict[str, str]):
    builder = builder.with_application_message_type("xfer")
    builder = builder.with_correlation_id(transfer_id)
    builder = builder.with_application_message_id(app_msg_id)
    for key, value in props.items():
        builder = builder.with_property(key, value)
    return builder


def require_meta(message) -> MetaPayload:
    meta, err = try_parse_meta(message)
    if meta is None:
        raise XferError(err)
    return meta
