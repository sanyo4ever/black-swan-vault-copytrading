from __future__ import annotations

import unittest

from bot.telegram_client import TelegramClientError, _raise_telegram_error


class TelegramClientErrorTests(unittest.TestCase):
    def test_retry_after_is_extracted_from_parameters(self) -> None:
        with self.assertRaises(TelegramClientError) as ctx:
            _raise_telegram_error(
                method="sendMessage",
                status_code=429,
                payload={
                    "ok": False,
                    "error_code": 429,
                    "description": "Too Many Requests: retry later",
                    "parameters": {"retry_after": 12},
                },
            )

        error = ctx.exception
        self.assertEqual(error.retry_after, 12)
        self.assertTrue(error.is_flood_limit())

    def test_retry_after_is_extracted_from_description(self) -> None:
        with self.assertRaises(TelegramClientError) as ctx:
            _raise_telegram_error(
                method="sendMessage",
                status_code=429,
                payload={
                    "ok": False,
                    "error_code": 429,
                    "description": "Too Many Requests: retry after 33",
                },
            )

        error = ctx.exception
        self.assertEqual(error.retry_after, 33)
        self.assertTrue(error.is_flood_limit())

    def test_topic_missing_and_chat_unavailable_classification(self) -> None:
        topic_missing = TelegramClientError(
            method="sendMessage",
            status_code=400,
            error_code=400,
            description="Bad Request: message thread not found",
        )
        self.assertTrue(topic_missing.is_topic_missing())
        self.assertFalse(topic_missing.is_chat_unavailable())

        blocked = TelegramClientError(
            method="sendMessage",
            status_code=403,
            error_code=403,
            description="Forbidden: bot was blocked by the user",
        )
        self.assertTrue(blocked.is_chat_unavailable())
        self.assertTrue(blocked.is_chat_blocked())

    def test_transient_classification(self) -> None:
        transient = TelegramClientError(
            method="sendMessage",
            status_code=502,
            error_code=502,
            description="Bad Gateway",
        )
        self.assertTrue(transient.is_transient())
        self.assertFalse(transient.is_chat_unavailable())


if __name__ == "__main__":
    unittest.main()
