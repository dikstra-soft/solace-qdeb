"""Non-destructive queue browse helper (tail-first pagination)."""

from __future__ import annotations

from typing import List, Optional

from solace.messaging.messaging_service import MessagingService
from solace.messaging.receiver.message_receiver import InboundMessage
from solace.messaging.resources.queue import Queue

RECEIVE_TIMEOUT_MS = 500
BROWSER_WINDOW_SIZE = 255


class QueueBrowser:
    """Maps tail-first item numbers (1 = newest) onto oldest-first API reads."""

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

    def scan_total_count(self) -> int:
        browser = self._create_browser()
        count = 0
        try:
            browser.start()
            while True:
                message = browser.receive_message(timeout=RECEIVE_TIMEOUT_MS)
                if message is None:
                    break
                count += 1
        finally:
            browser.terminate()
        return count

    def fetch_page(self, page: int, page_size: int, total_count: int) -> List[InboundMessage]:
        if total_count <= 0 or page < 1 or page_size < 1:
            return []

        start_item = (page - 1) * page_size + 1
        if start_item > total_count:
            return []

        end_item = min(page * page_size, total_count)
        skip_from_oldest = total_count - end_item
        take = end_item - start_item + 1

        browser = self._create_browser()
        try:
            browser.start()
            self._skip(browser, skip_from_oldest)
            batch = self._read(browser, take)
            batch.reverse()
            return batch
        finally:
            browser.terminate()

    def fetch_item(self, absolute_item: int, total_count: int) -> Optional[InboundMessage]:
        if total_count <= 0 or absolute_item < 1 or absolute_item > total_count:
            return None
        skip_from_oldest = total_count - absolute_item
        browser = self._create_browser()
        try:
            browser.start()
            self._skip(browser, skip_from_oldest)
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
