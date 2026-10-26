"""Non-destructive queue browse helper (oldest-first pagination)."""

from __future__ import annotations

from typing import List, Optional

from solace.messaging.messaging_service import MessagingService
from solace.messaging.receiver.message_receiver import InboundMessage
from solace.messaging.resources.queue import Queue

RECEIVE_TIMEOUT_MS = 500
BROWSER_WINDOW_SIZE = 255


class QueueBrowser:
    """Reads the queue in native oldest-first order (item #1 = oldest)."""

    def __init__(self, service: MessagingService, queue_name: str):
        self._service = service
        self._queue_name = queue_name

    def _create_browser(self):
        queue = Queue.durable_non_exclusive_queue(self._queue_name)
        return (
            self._service.create_message_queue_browser_builder()
            .with_queue_browser_window_size(BROWSER_WINDOW_SIZE)
            .build(queue)
        )

    def fetch_page(self, page: int, page_size: int) -> List[InboundMessage]:
        """Read one page from the head. A short page means end of queue."""
        if page < 1 or page_size < 1:
            return []

        skip = (page - 1) * page_size
        browser = self._create_browser()
        try:
            browser.start()
            self._skip(browser, skip)
            return self._read(browser, page_size)
        finally:
            browser.terminate()

    def for_each_from_oldest(self, limit: Optional[int] = None, cancelled=None):
        """Yield messages oldest-first. Caller should not retain them across iterations."""
        if limit is not None and limit <= 0:
            return
        delivered = 0
        browser = self._create_browser()
        try:
            browser.start()
            while True:
                if cancelled is not None and cancelled():
                    break
                if limit is not None and delivered >= limit:
                    break
                message = browser.receive_message(timeout=RECEIVE_TIMEOUT_MS)
                if message is None:
                    break
                yield message
                delivered += 1
        finally:
            browser.terminate()

    def fetch_item(self, absolute_item: int) -> Optional[InboundMessage]:
        """Read a single message by absolute item number (1 = oldest)."""
        if absolute_item < 1:
            return None
        browser = self._create_browser()
        try:
            browser.start()
            self._skip(browser, absolute_item - 1)
            return browser.receive_message(timeout=RECEIVE_TIMEOUT_MS)
        finally:
            browser.terminate()

    @staticmethod
    def _skip(browser, count: int) -> None:
        for _ in range(count):
            message = browser.receive_message(timeout=RECEIVE_TIMEOUT_MS)
            if message is None:
                break

    @staticmethod
    def _read(browser, count: int) -> List[InboundMessage]:
        messages: List[InboundMessage] = []
        for _ in range(count):
            message = browser.receive_message(timeout=RECEIVE_TIMEOUT_MS)
            if message is None:
                break
            messages.append(message)
        return messages
