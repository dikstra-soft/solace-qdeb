"""Minimal tests for defensive message field access and UTC timestamps."""

from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone

from common import payload_formatter


class _BrowseLikeMessage:
    """Simulates a browsed _InboundMessage missing optional getters."""

    def get_payload_as_bytes(self):
        return b"hello"

    def get_sender_timestamp(self):
        return 1_752_073_845_123

    def get_destination_name(self):
        return "test/q"

    def __str__(self) -> str:
        return "Destination: test/q"


class PayloadFormatterTests(unittest.TestCase):
    def test_build_json_line_without_optional_getters(self) -> None:
        line = payload_formatter.build_json_line(_BrowseLikeMessage())  # type: ignore[arg-type]
        record = json.loads(line)

        self.assertEqual(record["payload"], "hello")
        self.assertEqual(record["destination"], "test/q")
        self.assertEqual(record["senderTimestamp"], 1_752_073_845_123)
        self.assertEqual(
            record["senderTimestampIso"],
            payload_formatter._format_timestamp_iso_utc(1_752_073_845_123),
        )
        self.assertNotIn("timeToLive", record)
        self.assertIn("receivedAt", record)

    def test_format_timestamp_from_ms_uses_utc(self) -> None:
        ts = int(datetime(2026, 7, 9, 17, 30, 45, 123000, tzinfo=timezone.utc).timestamp() * 1000)
        formatted = payload_formatter._format_timestamp_from_ms(ts)
        self.assertTrue(formatted.endswith(" UTC"))
        self.assertIn("2026-07-09 17:30:45.123", formatted)

    def test_compact_browser_line_does_not_crash(self) -> None:
        line = payload_formatter.format_compact_browser_line(_BrowseLikeMessage(), 1)  # type: ignore[arg-type]
        self.assertIn("#1", line)
        self.assertIn("UTC", line)
        self.assertIn("5b", line)


if __name__ == "__main__":
    unittest.main()
