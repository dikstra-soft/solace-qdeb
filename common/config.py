"""Configuration loaded from .env / environment."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from common import dotenv


def parse_topics(raw: str | None) -> list[str]:
    """Split a comma-separated topic list: trim each segment, drop empties."""
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


class SourceType(str, Enum):
    TOPIC = "topic"
    QUEUE = "queue"


class ConsumeMode(str, Enum):
    CONTINUOUS = "continuous"
    STEP = "step"


class ReconnectMode(str, Enum):
    SEAMLESS = "seamless"
    STOP = "stop"


class LogFormat(str, Enum):
    FULL = "full"
    SIMPLE = "simple"
    PAYLOAD_ONLY = "payload_only"


def parse_log_format(raw: str | None) -> LogFormat:
    """Parse SOLACE_LOG_FORMAT: full/ndjson, simple/compact, or payload_only. Default full."""
    if raw is None or str(raw).strip() == "":
        return LogFormat.FULL
    normalized = str(raw).strip().lower()
    if normalized in ("full", "ndjson"):
        return LogFormat.FULL
    if normalized in ("simple", "compact"):
        return LogFormat.SIMPLE
    if normalized in ("payload_only", "payload-only", "payload", "body"):
        return LogFormat.PAYLOAD_ONLY
    raise ValueError(
        f"SOLACE_LOG_FORMAT must be 'full', 'simple', or 'payload_only', got '{raw}'."
    )


class DeliveryModeSetting(str, Enum):
    """Publisher delivery mode from env (auto resolves by destination type)."""

    AUTO = "auto"
    DIRECT = "direct"
    PERSISTENT = "persistent"


class MessageDeliveryKind(str, Enum):
    DIRECT = "direct"
    PERSISTENT = "persistent"


@dataclass
class AppConfig:
    host: str
    host_url: str
    vpn: str
    username: str
    password: str
    secure: bool
    ssl_validate_certificate: bool
    connect_timeout_ms: int
    reconnect_retries: int
    source: SourceType
    topic: str
    queue: str
    mode: ConsumeMode
    mode_explicit: bool
    reconnect_mode: ReconnectMode
    console_full: bool
    log_to_file: bool
    log_file: str
    log_format: LogFormat
    log_warn_bytes: int

    @staticmethod
    def _build_host_url(host: str, secure: bool) -> str:
        if "://" in host:
            return host
        scheme = "tcps" if secure else "tcp"
        return f"{scheme}://{host}"

    def resolve_mode(self, source: SourceType) -> ConsumeMode:
        """Effective mode for a destination type (env override or queue→step / topic→continuous)."""
        if self.mode_explicit:
            return self.mode
        return ConsumeMode.STEP if source == SourceType.QUEUE else ConsumeMode.CONTINUOUS

    @classmethod
    def load(cls, env_path: Optional[str] = None) -> Tuple["AppConfig", Optional[str]]:
        env_file, loaded_path = dotenv.load(env_path)
        return cls.from_env(env_file), loaded_path

    @classmethod
    def from_env(cls, env_file: dict) -> "AppConfig":
        def get(key: str, default: Optional[str] = None, *, required: bool = False) -> str:
            value = dotenv.get(key, env_file, required=required, default=default)
            if value is None:
                return ""
            return value

        def get_bool(key: str, fallback: bool) -> bool:
            raw = dotenv.get(key, env_file, default=None)
            if raw is None:
                return fallback
            normalized = raw.strip().lower()
            if normalized in ("1", "true", "yes", "y", "on"):
                return True
            if normalized in ("0", "false", "no", "n", "off"):
                return False
            raise ValueError(
                f"Setting '{key}' must be a boolean (true/false), got '{raw}'."
            )

        def get_int(key: str, fallback: int) -> int:
            raw = dotenv.get(key, env_file, default=None)
            if raw is None:
                return fallback
            try:
                return int(raw)
            except ValueError as exc:
                raise ValueError(
                    f"Setting '{key}' must be an integer, got '{raw}'."
                ) from exc

        host = get("SOLACE_HOST", required=True)
        secure = get_bool("SOLACE_SECURE", False)

        source_raw = (get("SOLACE_SOURCE_TYPE", default="topic") or "topic").lower()
        if source_raw == "topic":
            source = SourceType.TOPIC
        elif source_raw == "queue":
            source = SourceType.QUEUE
        else:
            raise ValueError(
                f"SOLACE_SOURCE_TYPE must be 'topic' or 'queue', got '{source_raw}'."
            )

        mode_raw = dotenv.get("SOLACE_CONSUME_MODE", env_file, default=None)
        mode_explicit = mode_raw is not None and str(mode_raw).strip() != ""
        if mode_explicit:
            normalized = str(mode_raw).strip().lower()
            if normalized in ("continuous", "normal", "stream"):
                mode = ConsumeMode.CONTINUOUS
            elif normalized in ("step", "paused", "manual", "single"):
                mode = ConsumeMode.STEP
            else:
                raise ValueError(
                    f"SOLACE_CONSUME_MODE must be 'continuous' or 'step', got '{mode_raw}'."
                )
        else:
            mode = (
                ConsumeMode.STEP
                if source == SourceType.QUEUE
                else ConsumeMode.CONTINUOUS
            )

        reconnect_raw = (
            get("SOLACE_RECONNECT_MODE", default="seamless") or "seamless"
        ).lower()
        if reconnect_raw in ("seamless", "continue"):
            reconnect_mode = ReconnectMode.SEAMLESS
        elif reconnect_raw in ("stop", "menu", "cancel"):
            reconnect_mode = ReconnectMode.STOP
        else:
            raise ValueError(
                f"SOLACE_RECONNECT_MODE must be 'seamless' or 'stop', got '{reconnect_raw}'."
            )

        warn_bytes_raw = dotenv.get("SOLACE_LOG_WARN_BYTES", env_file, default=None)
        if warn_bytes_raw is not None and str(warn_bytes_raw).strip() != "":
            try:
                log_warn_bytes = int(str(warn_bytes_raw).strip())
            except ValueError as exc:
                raise ValueError(
                    f"SOLACE_LOG_WARN_BYTES must be an integer, got '{warn_bytes_raw}'."
                ) from exc
            if log_warn_bytes < 0:
                raise ValueError("SOLACE_LOG_WARN_BYTES must be >= 0.")
        else:
            warn_mb = get_int("SOLACE_LOG_WARN_MB", 50)
            if warn_mb < 0:
                raise ValueError("SOLACE_LOG_WARN_MB must be >= 0.")
            log_warn_bytes = warn_mb * 1024 * 1024

        topic = get("SOLACE_TOPIC", default="")
        queue = get("SOLACE_QUEUE", default="")

        if source == SourceType.TOPIC and not parse_topics(topic):
            raise ValueError(
                "SOLACE_SOURCE_TYPE=topic requires SOLACE_TOPIC to be set to at least one non-empty topic."
            )
        if source == SourceType.QUEUE and not queue.strip():
            raise ValueError("SOLACE_SOURCE_TYPE=queue requires SOLACE_QUEUE to be set.")

        return cls(
            host=host,
            host_url=cls._build_host_url(host, secure),
            vpn=get("SOLACE_VPN", default="default") or "default",
            username=get("SOLACE_USERNAME", required=True),
            password=get("SOLACE_PASSWORD", default=""),
            secure=secure,
            ssl_validate_certificate=get_bool("SOLACE_SSL_VALIDATE_CERT", False),
            connect_timeout_ms=get_int("SOLACE_CONNECT_TIMEOUT_MS", 10000),
            reconnect_retries=get_int("SOLACE_RECONNECT_RETRIES", 3),
            source=source,
            topic=topic,
            queue=queue,
            mode=mode,
            mode_explicit=mode_explicit,
            reconnect_mode=reconnect_mode,
            console_full=get_bool("SOLACE_CONSOLE_FULL", False),
            log_to_file=get_bool("SOLACE_LOG_TO_FILE", True),
            log_file=get("SOLACE_LOG_FILE", default="solace-events.log") or "solace-events.log",
            log_format=parse_log_format(dotenv.get("SOLACE_LOG_FORMAT", env_file, default=None)),
            log_warn_bytes=log_warn_bytes,
        )

    def describe(self) -> str:
        if self.source == SourceType.TOPIC:
            topics = parse_topics(self.topic)
            if len(topics) <= 1:
                target = f"topic   = {topics[0] if topics else self.topic}"
            else:
                target = f"topics  = {len(topics)}: {', '.join(topics)}"
        else:
            target = f"queue   = {self.queue}"
        tls_note = ""
        if self.secure and not self.ssl_validate_certificate:
            tls_note = ", cert validation OFF"
        mode_text = (
            self.mode.value
            if self.mode_explicit
            else f"{self.mode.value} (auto: queue=step, topic=continuous)"
        )
        lines = [
            f"  host    = {self.host_url}  ({'secure SMF (TLS)' if self.secure else 'plain SMF'}{tls_note})",
            f"  vpn     = {self.vpn}",
            f"  user    = {self.username}",
            f"  source  = {self.source.value}",
            f"  {target}",
            f"  mode    = {mode_text}",
            f"  reconnect = {self.reconnect_mode.value}",
            f"  console = {'full' if self.console_full else 'simple'}",
            f"  logfile = {self.log_file if self.log_to_file else 'off'}",
            f"  logfmt  = {self.log_format.value}",
        ]
        return "\n".join(lines)


@dataclass
class BrowserAppConfig:
    host: str
    host_url: str
    vpn: str
    username: str
    password: str
    secure: bool
    ssl_validate_certificate: bool
    connect_timeout_ms: int
    reconnect_retries: int
    default_queue: str
    default_page_size: int
    console_full: bool
    log_to_file: bool
    log_file: str
    log_format: LogFormat

    @classmethod
    def load(cls, env_path: Optional[str] = None) -> Tuple["BrowserAppConfig", Optional[str]]:
        env_file, loaded_path = dotenv.load(env_path)
        return cls.from_env(env_file), loaded_path

    @classmethod
    def from_env(cls, env_file: dict) -> "BrowserAppConfig":
        app = AppConfig.from_env(env_file)
        page_size_raw = dotenv.get("SOLACE_BROWSER_PAGE_SIZE", env_file, default="10")
        try:
            page_size = int(page_size_raw or "10")
        except ValueError as exc:
            raise ValueError(
                f"SOLACE_BROWSER_PAGE_SIZE must be an integer, got '{page_size_raw}'."
            ) from exc
        if page_size <= 0:
            raise ValueError("SOLACE_BROWSER_PAGE_SIZE must be a positive integer.")

        return cls(
            host=app.host,
            host_url=app.host_url,
            vpn=app.vpn,
            username=app.username,
            password=app.password,
            secure=app.secure,
            ssl_validate_certificate=app.ssl_validate_certificate,
            connect_timeout_ms=app.connect_timeout_ms,
            reconnect_retries=app.reconnect_retries,
            default_queue=app.queue,
            default_page_size=page_size,
            console_full=app.console_full,
            log_to_file=app.log_to_file,
            log_file=app.log_file,
            log_format=app.log_format,
        )

    def to_connection_config(self, queue: str) -> AppConfig:
        return AppConfig(
            host=self.host,
            host_url=self.host_url,
            vpn=self.vpn,
            username=self.username,
            password=self.password,
            secure=self.secure,
            ssl_validate_certificate=self.ssl_validate_certificate,
            connect_timeout_ms=self.connect_timeout_ms,
            reconnect_retries=self.reconnect_retries,
            source=SourceType.QUEUE,
            topic="",
            queue=queue,
            mode=ConsumeMode.CONTINUOUS,
            mode_explicit=True,
            reconnect_mode=ReconnectMode.SEAMLESS,
            console_full=self.console_full,
            log_to_file=self.log_to_file,
            log_file=self.log_file,
            log_format=self.log_format,
            log_warn_bytes=0,
        )

    def describe(self, queue: str, page_size: int) -> str:
        tls_note = ""
        if self.secure and not self.ssl_validate_certificate:
            tls_note = ", cert validation OFF"
        lines = [
            f"  host      = {self.host_url}  ({'secure SMF (TLS)' if self.secure else 'plain SMF'}{tls_note})",
            f"  vpn       = {self.vpn}",
            f"  user      = {self.username}",
            f"  queue     = {queue}",
            f"  page size = {page_size}",
            f"  console   = {'full' if self.console_full else 'simple'}",
            f"  logfile   = {self.log_file if self.log_to_file else 'off'}",
            f"  logfmt    = {self.log_format.value}",
        ]
        return "\n".join(lines)


@dataclass
class PublisherAppConfig:
    """Config for the publisher tool (connection + dest defaults + delivery mode)."""

    host: str
    host_url: str
    vpn: str
    username: str
    password: str
    secure: bool
    ssl_validate_certificate: bool
    connect_timeout_ms: int
    reconnect_retries: int
    source: SourceType
    topic: str
    queue: str
    delivery_mode: DeliveryModeSetting

    @classmethod
    def load(cls, env_path: Optional[str] = None) -> Tuple["PublisherAppConfig", Optional[str]]:
        env_file, loaded_path = dotenv.load(env_path)
        return cls.from_env(env_file), loaded_path

    @classmethod
    def from_env(cls, env_file: dict) -> "PublisherAppConfig":
        def get(key: str, default: Optional[str] = None, *, required: bool = False) -> str:
            value = dotenv.get(key, env_file, required=required, default=default)
            if value is None:
                return ""
            return value

        def get_bool(key: str, fallback: bool) -> bool:
            raw = dotenv.get(key, env_file, default=None)
            if raw is None:
                return fallback
            normalized = raw.strip().lower()
            if normalized in ("1", "true", "yes", "y", "on"):
                return True
            if normalized in ("0", "false", "no", "n", "off"):
                return False
            raise ValueError(
                f"Setting '{key}' must be a boolean (true/false), got '{raw}'."
            )

        def get_int(key: str, fallback: int) -> int:
            raw = dotenv.get(key, env_file, default=None)
            if raw is None:
                return fallback
            try:
                return int(raw)
            except ValueError as exc:
                raise ValueError(
                    f"Setting '{key}' must be an integer, got '{raw}'."
                ) from exc

        host = get("SOLACE_HOST", required=True)
        secure = get_bool("SOLACE_SECURE", False)

        source_raw = (get("SOLACE_SOURCE_TYPE", default="topic") or "topic").lower()
        if source_raw == "topic":
            source = SourceType.TOPIC
        elif source_raw == "queue":
            source = SourceType.QUEUE
        else:
            raise ValueError(
                f"SOLACE_SOURCE_TYPE must be 'topic' or 'queue', got '{source_raw}'."
            )

        delivery_raw = dotenv.get("SOLACE_DELIVERY_MODE", env_file, default=None)
        if delivery_raw is None or str(delivery_raw).strip() == "":
            delivery = DeliveryModeSetting.AUTO
        else:
            normalized = str(delivery_raw).strip().lower()
            if normalized == "auto":
                delivery = DeliveryModeSetting.AUTO
            elif normalized == "direct":
                delivery = DeliveryModeSetting.DIRECT
            elif normalized in ("persistent", "guaranteed"):
                delivery = DeliveryModeSetting.PERSISTENT
            else:
                raise ValueError(
                    "SOLACE_DELIVERY_MODE must be 'direct', 'persistent', or 'auto', "
                    f"got '{delivery_raw}'."
                )

        return cls(
            host=host,
            host_url=AppConfig._build_host_url(host, secure),
            vpn=get("SOLACE_VPN", default="default") or "default",
            username=get("SOLACE_USERNAME", required=True),
            password=get("SOLACE_PASSWORD", default=""),
            secure=secure,
            ssl_validate_certificate=get_bool("SOLACE_SSL_VALIDATE_CERT", False),
            connect_timeout_ms=get_int("SOLACE_CONNECT_TIMEOUT_MS", 10000),
            reconnect_retries=get_int("SOLACE_RECONNECT_RETRIES", 3),
            source=source,
            topic=get("SOLACE_TOPIC", default=""),
            queue=get("SOLACE_QUEUE", default=""),
            delivery_mode=delivery,
        )

    def to_connection_config(self) -> AppConfig:
        return AppConfig(
            host=self.host,
            host_url=self.host_url,
            vpn=self.vpn,
            username=self.username,
            password=self.password,
            secure=self.secure,
            ssl_validate_certificate=self.ssl_validate_certificate,
            connect_timeout_ms=self.connect_timeout_ms,
            reconnect_retries=self.reconnect_retries,
            source=self.source,
            topic=self.topic,
            queue=self.queue,
            mode=ConsumeMode.CONTINUOUS,
            mode_explicit=True,
            reconnect_mode=ReconnectMode.SEAMLESS,
            console_full=False,
            log_to_file=False,
            log_file="solace-events.log",
            log_format=LogFormat.FULL,
            log_warn_bytes=0,
        )

    def resolve_delivery(self, destination_type: SourceType) -> MessageDeliveryKind:
        if self.delivery_mode == DeliveryModeSetting.DIRECT:
            return MessageDeliveryKind.DIRECT
        if self.delivery_mode == DeliveryModeSetting.PERSISTENT:
            return MessageDeliveryKind.PERSISTENT
        return (
            MessageDeliveryKind.PERSISTENT
            if destination_type == SourceType.QUEUE
            else MessageDeliveryKind.DIRECT
        )

    def describe(
        self,
        dest_type: SourceType,
        dest_name: str,
        delivery: MessageDeliveryKind,
    ) -> str:
        tls_note = ""
        if self.secure and not self.ssl_validate_certificate:
            tls_note = ", cert validation OFF"
        auto_note = " (auto)" if self.delivery_mode == DeliveryModeSetting.AUTO else ""
        lines = [
            f"  host     = {self.host_url}  ({'secure SMF (TLS)' if self.secure else 'plain SMF'}{tls_note})",
            f"  vpn      = {self.vpn}",
            f"  user     = {self.username}",
            f"  dest     = {dest_type.value} '{dest_name}'",
            f"  delivery = {delivery.value}{auto_note}",
        ]
        return "\n".join(lines)


@dataclass
class XferAppConfig:
    """Config for solace-xfer. Connection keys plus optional chunk size."""

    DEFAULT_CHUNK_BYTES = 1024 * 1024
    MAX_CHUNK_BYTES = 10_000_000

    host: str
    host_url: str
    vpn: str
    username: str
    password: str
    secure: bool
    ssl_validate_certificate: bool
    connect_timeout_ms: int
    reconnect_retries: int
    topic: str
    queue: str
    chunk_bytes: int

    @classmethod
    def load(cls, env_path: Optional[str] = None) -> Tuple["XferAppConfig", Optional[str]]:
        env_file, loaded_path = dotenv.load(env_path)
        return cls.from_env(env_file), loaded_path

    @classmethod
    def from_env(cls, env_file: dict) -> "XferAppConfig":
        def get(key: str, default: Optional[str] = None, *, required: bool = False) -> str:
            value = dotenv.get(key, env_file, required=required, default=default)
            if value is None:
                return ""
            return value

        def get_bool(key: str, fallback: bool) -> bool:
            raw = dotenv.get(key, env_file, default=None)
            if raw is None:
                return fallback
            normalized = raw.strip().lower()
            if normalized in ("1", "true", "yes", "y", "on"):
                return True
            if normalized in ("0", "false", "no", "n", "off"):
                return False
            raise ValueError(
                f"Setting '{key}' must be a boolean (true/false), got '{raw}'."
            )

        def get_int(key: str, fallback: int) -> int:
            raw = dotenv.get(key, env_file, default=None)
            if raw is None:
                return fallback
            try:
                return int(raw)
            except ValueError as exc:
                raise ValueError(
                    f"Setting '{key}' must be an integer, got '{raw}'."
                ) from exc

        host = get("SOLACE_HOST", required=True)
        secure = get_bool("SOLACE_SECURE", False)
        chunk = get_int("SOLACE_XFER_CHUNK_BYTES", cls.DEFAULT_CHUNK_BYTES)
        if chunk < 1 or chunk > cls.MAX_CHUNK_BYTES:
            raise ValueError(
                f"SOLACE_XFER_CHUNK_BYTES must be between 1 and {cls.MAX_CHUNK_BYTES} "
                f"(Solace common max payload), got {chunk}."
            )

        return cls(
            host=host,
            host_url=AppConfig._build_host_url(host, secure),
            vpn=get("SOLACE_VPN", default="default") or "default",
            username=get("SOLACE_USERNAME", required=True),
            password=get("SOLACE_PASSWORD", default=""),
            secure=secure,
            ssl_validate_certificate=get_bool("SOLACE_SSL_VALIDATE_CERT", False),
            connect_timeout_ms=get_int("SOLACE_CONNECT_TIMEOUT_MS", 10000),
            reconnect_retries=get_int("SOLACE_RECONNECT_RETRIES", 3),
            topic=get("SOLACE_TOPIC", default=""),
            queue=get("SOLACE_QUEUE", default=""),
            chunk_bytes=chunk,
        )

    def to_connection_config(self) -> AppConfig:
        return AppConfig(
            host=self.host,
            host_url=self.host_url,
            vpn=self.vpn,
            username=self.username,
            password=self.password,
            secure=self.secure,
            ssl_validate_certificate=self.ssl_validate_certificate,
            connect_timeout_ms=self.connect_timeout_ms,
            reconnect_retries=self.reconnect_retries,
            source=SourceType.QUEUE,
            topic=self.topic,
            queue=self.queue,
            mode=ConsumeMode.CONTINUOUS,
            mode_explicit=True,
            reconnect_mode=ReconnectMode.SEAMLESS,
            console_full=False,
            log_to_file=False,
            log_file="solace-events.log",
            log_format=LogFormat.FULL,
            log_warn_bytes=0,
        )

    def describe(self) -> str:
        tls_note = ""
        if self.secure and not self.ssl_validate_certificate:
            tls_note = ", cert validation OFF"
        lines = [
            f"  host       = {self.host_url}  ({'secure SMF (TLS)' if self.secure else 'plain SMF'}{tls_note})",
            f"  vpn        = {self.vpn}",
            f"  user       = {self.username}",
            f"  chunkBytes = {self.chunk_bytes}",
        ]
        return "\n".join(lines)
