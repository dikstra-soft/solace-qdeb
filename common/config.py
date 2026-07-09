"""Configuration loaded from .env / environment."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple

from common import dotenv


class SourceType(str, Enum):
    TOPIC = "topic"
    QUEUE = "queue"


class ConsumeMode(str, Enum):
    CONTINUOUS = "continuous"
    STEP = "step"


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
    console_full: bool
    log_to_file: bool
    log_file: str

    @staticmethod
    def _build_host_url(host: str, secure: bool) -> str:
        if "://" in host:
            return host
        scheme = "tcps" if secure else "tcp"
        return f"{scheme}://{host}"

    @classmethod
    def load(cls) -> Tuple["AppConfig", Optional[str]]:
        env_file, loaded_path = dotenv.load()
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

        mode_raw = (get("SOLACE_CONSUME_MODE", default="continuous") or "continuous").lower()
        if mode_raw in ("continuous", "normal", "stream"):
            mode = ConsumeMode.CONTINUOUS
        elif mode_raw in ("step", "paused", "manual", "single"):
            mode = ConsumeMode.STEP
        else:
            raise ValueError(
                f"SOLACE_CONSUME_MODE must be 'continuous' or 'step', got '{mode_raw}'."
            )

        topic = get("SOLACE_TOPIC", default="")
        queue = get("SOLACE_QUEUE", default="")

        if source == SourceType.TOPIC and not topic.strip():
            raise ValueError("SOLACE_SOURCE_TYPE=topic requires SOLACE_TOPIC to be set.")
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
            console_full=get_bool("SOLACE_CONSOLE_FULL", False),
            log_to_file=get_bool("SOLACE_LOG_TO_FILE", True),
            log_file=get("SOLACE_LOG_FILE", default="solace-events.log") or "solace-events.log",
        )

    def describe(self) -> str:
        target = (
            f"topic   = {self.topic}"
            if self.source == SourceType.TOPIC
            else f"queue   = {self.queue}"
        )
        tls_note = ""
        if self.secure and not self.ssl_validate_certificate:
            tls_note = ", cert validation OFF"
        lines = [
            f"  host    = {self.host_url}  ({'secure SMF (TLS)' if self.secure else 'plain SMF'}{tls_note})",
            f"  vpn     = {self.vpn}",
            f"  user    = {self.username}",
            f"  source  = {self.source.value}",
            f"  {target}",
            f"  mode    = {self.mode.value}",
            f"  console = {'full' if self.console_full else 'simple'}",
            f"  logfile = {self.log_file if self.log_to_file else 'off'}",
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

    @classmethod
    def load(cls) -> Tuple["BrowserAppConfig", Optional[str]]:
        env_file, loaded_path = dotenv.load()
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
            console_full=self.console_full,
            log_to_file=self.log_to_file,
            log_file=self.log_file,
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
        ]
        return "\n".join(lines)
