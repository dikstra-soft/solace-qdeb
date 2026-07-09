"""Solace MessagingService connection helper."""

from __future__ import annotations

from typing import Optional

from solace.messaging.config.solace_properties import authentication_properties as auth_props
from solace.messaging.config.solace_properties import service_properties
from solace.messaging.config.solace_properties import transport_layer_properties as transport_props
from solace.messaging.config.transport_security_strategy import TLS
from solace.messaging.messaging_service import MessagingService, RetryStrategy

from common.config import AppConfig


class SolaceConnection:
    """Owns a connected MessagingService."""

    def __init__(self, service: MessagingService):
        self.service = service

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

    def disconnect(self) -> None:
        try:
            if self.service.is_connected:
                self.service.disconnect()
        except Exception:
            pass
