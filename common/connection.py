"""Solace MessagingService connection helper."""

from __future__ import annotations

from typing import Callable, Optional

from solace.messaging.config.solace_properties import authentication_properties as auth_props
from solace.messaging.config.solace_properties import service_properties
from solace.messaging.config.solace_properties import transport_layer_properties as transport_props
from solace.messaging.config.transport_security_strategy import TLS
from solace.messaging.messaging_service import (
    MessagingService,
    ReconnectionAttemptListener,
    ReconnectionListener,
    RetryStrategy,
    ServiceEvent,
    ServiceInterruptionListener,
)

from common.config import AppConfig


class SolaceConnection:
    """Owns a connected MessagingService."""

    def __init__(self, service: MessagingService):
        self.service = service
        self._reconnect_listener: Optional[ReconnectionListener] = None
        self._attempt_listener: Optional[ReconnectionAttemptListener] = None
        self._interrupt_listener: Optional[ServiceInterruptionListener] = None

    @classmethod
    def connect(cls, config: AppConfig) -> "SolaceConnection":
        broker_props = {
            transport_props.HOST: config.host_url,
            service_properties.VPN_NAME: config.vpn,
            auth_props.SCHEME_BASIC_USER_NAME: config.username,
            auth_props.SCHEME_BASIC_PASSWORD: config.password,
            transport_props.CONNECTION_ATTEMPTS_TIMEOUT: config.connect_timeout_ms,
            transport_props.RECONNECTION_ATTEMPTS: config.reconnect_retries,
        }

        builder = MessagingService.builder().from_properties(broker_props)

        if config.secure:
            tls = TLS.create()
            if config.ssl_validate_certificate:
                tls = tls.with_certificate_validation(False, True, None, None)
            else:
                tls = tls.without_certificate_validation()
            builder = builder.with_transport_security_strategy(tls)

        if config.reconnect_retries > 0:
            builder = builder.with_reconnection_retry_strategy(
                RetryStrategy.parametrized_retry(config.reconnect_retries, 3)
            )

        service = builder.build()
        service.connect()
        if not service.is_connected:
            raise RuntimeError(f"Failed to connect to broker ({config.host_url}).")

        return cls(service)

    def set_reconnect_handlers(
        self,
        *,
        on_reconnecting: Optional[Callable[[ServiceEvent], None]] = None,
        on_reconnected: Optional[Callable[[ServiceEvent], None]] = None,
        on_interrupted: Optional[Callable[[ServiceEvent], None]] = None,
    ) -> None:
        """Register (or replace) session reconnection / interruption listeners."""
        self.clear_reconnect_handlers()

        if on_reconnecting is not None:

            class AttemptListener(ReconnectionAttemptListener):
                def on_reconnecting(self, event: ServiceEvent) -> None:
                    on_reconnecting(event)

            self._attempt_listener = AttemptListener()
            self.service.add_reconnection_attempt_listener(self._attempt_listener)

        if on_reconnected is not None:

            class DoneListener(ReconnectionListener):
                def on_reconnected(self, service_event: ServiceEvent) -> None:
                    on_reconnected(service_event)

            self._reconnect_listener = DoneListener()
            self.service.add_reconnection_listener(self._reconnect_listener)

        if on_interrupted is not None:

            class InterruptListener(ServiceInterruptionListener):
                def on_service_interrupted(self, event: ServiceEvent) -> None:
                    on_interrupted(event)

            self._interrupt_listener = InterruptListener()
            self.service.add_service_interruption_listener(self._interrupt_listener)

    def clear_reconnect_handlers(self) -> None:
        if self._attempt_listener is not None:
            try:
                self.service.remove_reconnection_attempt_listener(self._attempt_listener)
            except Exception:
                pass
            self._attempt_listener = None
        if self._reconnect_listener is not None:
            try:
                self.service.remove_reconnection_listener(self._reconnect_listener)
            except Exception:
                pass
            self._reconnect_listener = None
        if self._interrupt_listener is not None:
            try:
                self.service.remove_service_interruption_listener(self._interrupt_listener)
            except Exception:
                pass
            self._interrupt_listener = None

    def disconnect(self) -> None:
        self.clear_reconnect_handlers()
        try:
            if self.service.is_connected:
                self.service.disconnect()
        except Exception:
            pass
