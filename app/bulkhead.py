import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager

from app.errors import ApiError, ErrorCode


class Bulkhead:
    """Caps concurrent work per key (coupon code) within one process.

    This is NOT a correctness mechanism: Postgres row locks and constraints decide every
    outcome, across all instances. The bulkhead only controls *how requests queue*. Without it,
    a hot coupon's lock queue can hold every pooled connection and starve all other coupons.
    Waiting here costs no DB connection and no lock; waiting too long returns 503 + Retry-After
    with nothing persisted, so the client retries with the same Idempotency-Key.
    """

    def __init__(self, limit: int, timeout_s: float):
        self._limit = limit
        self._timeout = timeout_s
        self._sems: dict[str, asyncio.Semaphore] = {}
        self._users: dict[str, int] = defaultdict(int)

    @asynccontextmanager
    async def slot(self, key: str):
        sem = self._sems.setdefault(key, asyncio.Semaphore(self._limit))
        self._users[key] += 1
        try:
            try:
                await asyncio.wait_for(sem.acquire(), self._timeout)
            except TimeoutError:
                raise ApiError(
                    ErrorCode.RETRYABLE,
                    "Coupon is under heavy load; nothing was applied, retry with the same Idempotency-Key",
                    headers={"Retry-After": "1"},
                ) from None
            try:
                yield
            finally:
                sem.release()
        finally:
            self._users[key] -= 1
            if self._users[key] == 0:  # no holders or waiters left: drop the entry (bounded memory)
                del self._users[key]
                del self._sems[key]
