"""Exclusive queue provision / deprovision via CCSMP (Python API has no public Deprovision)."""

from __future__ import annotations

import ctypes
from ctypes import POINTER, c_char_p, c_int
from dataclasses import dataclass
from typing import Optional

import solace
from solace.messaging.config._sol_constants import (
    SOLCLIENT_ENDPOINT_PERM_CONSUME,
    SOLCLIENT_ENDPOINT_PROP_ACCESSTYPE,
    SOLCLIENT_ENDPOINT_PROP_ACCESSTYPE_EXCLUSIVE,
    SOLCLIENT_ENDPOINT_PROP_DURABLE,
    SOLCLIENT_ENDPOINT_PROP_ID,
    SOLCLIENT_ENDPOINT_PROP_NAME,
    SOLCLIENT_ENDPOINT_PROP_PERMISSION,
    SOLCLIENT_ENDPOINT_PROP_QUEUE,
    SOLCLIENT_OK,
    SOLCLIENT_PROP_ENABLE_VAL,
    SOLCLIENT_PROVISION_FLAGS_IGNORE_EXIST_ERRORS,
    SOLCLIENT_PROVISION_FLAGS_WAITFORCONFIRM,
)
from solace.messaging.config.solace_constants import receiver_constants as ack_constants
from solace.messaging.config.solace_properties.receiver_properties import (
    PERSISTENT_MESSAGE_ACK_STRATEGY,
)
from solace.messaging.core._core_api_utility import prepare_array
from solace.messaging.messaging_service import MessagingService
from solace.messaging.receiver._inbound_message_utility import end_point_provision
from solace.messaging.resources.queue import Queue
from solace.messaging._solace_logging._core_api_log import last_error_info
from solace.messaging.config._solace_message_constants import CCSMP_INFO_SUB_CODE, CCSMP_SUB_CODE

try:
    CLIENT_ACK = ack_constants.PERSISTENT_RECEIVER_CLIENT_ACK
except Exception:  # pragma: no cover
    CLIENT_ACK = "PERSISTENT_RECEIVER_CLIENT_ACK"

SUBCODE_PERMISSION = "SOLCLIENT_SUBCODE_PERMISSION_NOT_ALLOWED"
SUBCODE_ALREADY = "SOLCLIENT_SUBCODE_ENDPOINT_ALREADY_EXISTS"
SUBCODE_MISMATCH = "SOLCLIENT_SUBCODE_ENDPOINT_PROPERTY_MISMATCH"


@dataclass
class QueueEnsureResult:
    ok: bool
    created_by_us: bool
    missing_and_cannot_create: bool
    error: Optional[str] = None

    @staticmethod
    def created() -> "QueueEnsureResult":
        return QueueEnsureResult(True, True, False, None)

    @staticmethod
    def existing() -> "QueueEnsureResult":
        return QueueEnsureResult(True, False, False, None)

    @staticmethod
    def fail(error: str, missing: bool) -> "QueueEnsureResult":
        return QueueEnsureResult(False, False, missing, error)


def _session_pointer(service: MessagingService):
    ptr = getattr(service, "session_pointer", None)
    if ptr is not None:
        return ptr
    session = getattr(service, "_session", None)
    if session is not None:
        return getattr(session, "session_pointer", None)
    return None


def _endpoint_array(queue_name: str):
    props = {
        SOLCLIENT_ENDPOINT_PROP_ID: SOLCLIENT_ENDPOINT_PROP_QUEUE,
        SOLCLIENT_ENDPOINT_PROP_DURABLE: SOLCLIENT_PROP_ENABLE_VAL,
        SOLCLIENT_ENDPOINT_PROP_ACCESSTYPE: SOLCLIENT_ENDPOINT_PROP_ACCESSTYPE_EXCLUSIVE,
        SOLCLIENT_ENDPOINT_PROP_PERMISSION: SOLCLIENT_ENDPOINT_PERM_CONSUME,
        SOLCLIENT_ENDPOINT_PROP_NAME: queue_name,
    }
    return prepare_array(props)


def _error_detail(return_code: int) -> str:
    try:
        info = last_error_info(status_code=return_code, caller_desc="endpoint provision")
    except Exception:
        return f"return code {return_code}"
    sub = info.get(CCSMP_SUB_CODE) if isinstance(info, dict) else None
    extra = info.get(CCSMP_INFO_SUB_CODE) if isinstance(info, dict) else None
    if sub == SUBCODE_PERMISSION or (isinstance(sub, str) and "PERMISSION" in sub.upper()):
        return f"permission denied (client profile needs endpoint create). {extra or ''}".strip()
    if sub:
        return f"{sub}: {extra or return_code}"
    return str(extra or return_code)


def queue_exists(service: MessagingService, queue_name: str) -> bool:
    browser = None
    try:
        queue = Queue.durable_exclusive_queue(queue_name)
        browser = (
            service.create_message_queue_browser_builder()
            .with_queue_browser_window_size(1)
            .build(queue)
        )
        browser.start()
        return True
    except Exception:
        try:
            queue = Queue.durable_non_exclusive_queue(queue_name)
            browser = (
                service.create_message_queue_browser_builder()
                .with_queue_browser_window_size(1)
                .build(queue)
            )
            browser.start()
            return True
        except Exception:
            return False
    finally:
        if browser is not None:
            try:
                browser.terminate()
            except Exception:
                pass


def is_empty(service: MessagingService, queue_name: str) -> bool:
    browser = None
    try:
        queue = Queue.durable_exclusive_queue(queue_name)
        browser = (
            service.create_message_queue_browser_builder()
            .with_queue_browser_window_size(1)
            .build(queue)
        )
        browser.start()
        message = browser.receive_message(timeout=250)
        return message is None
    except Exception:
        return False
    finally:
        if browser is not None:
            try:
                browser.terminate()
            except Exception:
                pass


def ensure_exclusive_queue(service: MessagingService, queue_name: str) -> QueueEnsureResult:
    if queue_exists(service, queue_name):
        return QueueEnsureResult.existing()

    session_p = _session_pointer(service)
    if session_p is None:
        return QueueEnsureResult.fail(
            f"cannot provision queue '{queue_name}': session pointer unavailable",
            missing=True,
        )

    try:
        arr = _endpoint_array(queue_name)
        rc = end_point_provision(arr, session_p, ignore_already_provisioned=False)
        if rc == SOLCLIENT_OK:
            return QueueEnsureResult.created()

        detail = _error_detail(rc)
        if queue_exists(service, queue_name):
            return QueueEnsureResult.existing()
        if SUBCODE_ALREADY in detail or SUBCODE_MISMATCH in detail:
            return QueueEnsureResult.existing()
        return QueueEnsureResult.fail(
            f"cannot provision queue '{queue_name}': {detail}",
            missing=True,
        )
    except Exception as exc:
        if queue_exists(service, queue_name):
            return QueueEnsureResult.existing()
        return QueueEnsureResult.fail(
            f"cannot provision queue '{queue_name}': {exc}",
            missing=True,
        )


def deprovision(service: MessagingService, queue_name: str) -> None:
    session_p = _session_pointer(service)
    if session_p is None:
        raise RuntimeError("session pointer unavailable")
    flags = SOLCLIENT_PROVISION_FLAGS_WAITFORCONFIRM | SOLCLIENT_PROVISION_FLAGS_IGNORE_EXIST_ERRORS
    arr = _endpoint_array(queue_name)
    rc = solace.CORE_LIB.solClient_session_endpointDeprovision(
        ctypes.cast(arr, POINTER(c_char_p)),
        session_p,
        c_int(flags),
        c_char_p(None),
    )
    if rc != SOLCLIENT_OK:
        raise RuntimeError(_error_detail(rc))


def build_hold_receiver(service: MessagingService, queue_name: str):
    queue = Queue.durable_exclusive_queue(queue_name)
    return (
        service.create_persistent_message_receiver_builder()
        .from_properties({PERSISTENT_MESSAGE_ACK_STRATEGY: CLIENT_ACK})
        .build(queue)
    )


def peek_browser(service: MessagingService, queue_name: str):
    last_error = None
    for factory in (Queue.durable_exclusive_queue, Queue.durable_non_exclusive_queue):
        try:
            queue = factory(queue_name)
            browser = (
                service.create_message_queue_browser_builder()
                .with_queue_browser_window_size(1)
                .build(queue)
            )
            return browser
        except Exception as exc:
            last_error = exc
    raise last_error or RuntimeError(f"cannot browse queue '{queue_name}'")
