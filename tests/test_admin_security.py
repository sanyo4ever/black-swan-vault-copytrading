from __future__ import annotations

import unittest

from bot.admin_server import _InMemoryRateLimiter, _rate_limit_scope


class _FakeRequest:
    def __init__(self, path: str) -> None:
        self.path = path


class AdminSecurityTests(unittest.TestCase):
    def test_rate_limiter_blocks_after_limit_and_recovers(self) -> None:
        limiter = _InMemoryRateLimiter(window_seconds=10, max_requests=2)
        self.assertEqual(limiter.allow(key="admin:1.2.3.4", now=0), (True, 0))
        self.assertEqual(limiter.allow(key="admin:1.2.3.4", now=1), (True, 0))

        allowed, retry_after = limiter.allow(key="admin:1.2.3.4", now=2)
        self.assertFalse(allowed)
        self.assertGreaterEqual(retry_after, 1)

        self.assertEqual(limiter.allow(key="admin:1.2.3.4", now=11), (True, 0))

    def test_rate_limiter_cleanup_drops_stale_keys(self) -> None:
        limiter = _InMemoryRateLimiter(window_seconds=5, max_requests=2, max_keys=100)
        for idx in range(120):
            self.assertEqual(limiter.allow(key=f"public:{idx}", now=0), (True, 0))

        limiter._cleanup(cutoff=10)
        self.assertLessEqual(len(limiter._buckets), limiter.max_keys)

    def test_rate_limit_scope(self) -> None:
        self.assertEqual(_rate_limit_scope(_FakeRequest("/admin")), "admin")
        self.assertEqual(_rate_limit_scope(_FakeRequest("/admin/discover")), "admin")
        self.assertEqual(_rate_limit_scope(_FakeRequest("/subscribe/0xabc")), "public")
        self.assertEqual(_rate_limit_scope(_FakeRequest("/telegram/0xabc/view")), "public")
        self.assertEqual(_rate_limit_scope(_FakeRequest("/api/traders")), "public")
        self.assertIsNone(_rate_limit_scope(_FakeRequest("/")))


if __name__ == "__main__":
    unittest.main()
