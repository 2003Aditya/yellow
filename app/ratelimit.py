import logging
from dataclasses import dataclass

from fastapi import Depends, Request
from psycopg_pool import AsyncConnectionPool

from app.config import Settings
from app.errors import ApiError, ErrorCode
from app.models import RedeemIn

log = logging.getLogger("coupons.ratelimit")

# Fixed-window counters for several buckets in ONE statement: rows are upserted in sorted
# bucket order (consistent lock order, no deadlocks) and the implicit transaction ends right
# away, so no limiter row lock is ever held while waiting for another.
HIT_SQL = """
INSERT INTO rate_limits AS rl (bucket, window_start, hits)
SELECT b, now(), 1 FROM unnest(%(buckets)s::text[]) AS b ORDER BY b
ON CONFLICT (bucket) DO UPDATE SET
    window_start = CASE WHEN rl.window_start <= now() - make_interval(secs => %(window)s)
                        THEN now() ELSE rl.window_start END,
    hits         = CASE WHEN rl.window_start <= now() - make_interval(secs => %(window)s)
                        THEN 1 ELSE rl.hits + 1 END
RETURNING bucket, hits,
          GREATEST(1, CEIL(EXTRACT(EPOCH FROM (window_start + make_interval(secs => %(window)s) - now()))))::int
              AS retry_after
"""


@dataclass(frozen=True)
class Limit:
    bucket: str
    limit: int


class RateLimiter:
    def __init__(self, pool: AsyncConnectionPool, settings: Settings):
        self.pool = pool  # dedicated pool: checkout load can't starve the limiter, or vice versa
        self.settings = settings

    async def check(self, limits: list[Limit]) -> None:
        """Count one hit on every bucket; 429 if any bucket is over its limit.

        Fails open: the limiter protects capacity, not correctness, so a limiter failure
        (DB down, limiter pool exhausted) must not take checkout down with it.
        """
        if not self.settings.rate_limit_enabled or not limits:
            return
        by_bucket = {l.bucket: l.limit for l in limits}
        try:
            async with self.pool.connection(timeout=self.settings.rate_limit_timeout_s) as conn:
                cur = await conn.execute(
                    HIT_SQL, {"buckets": sorted(by_bucket), "window": self.settings.rate_limit_window_s}
                )
                rows = await cur.fetchall()
        except Exception:
            log.warning("rate limiter unavailable, failing open", exc_info=True)
            return
        over = [(bucket, retry) for bucket, hits, retry in rows if hits > by_bucket[bucket]]
        if over:
            bucket, retry_after = max(over, key=lambda o: o[1])
            raise ApiError(
                ErrorCode.RATE_LIMITED,
                f"Rate limit exceeded ({bucket.rsplit(':', 1)[0]}); retry in {retry_after}s",
                headers={"Retry-After": str(retry_after)},
            )


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def limit_by_ip(scope: str, setting: str):
    """Route dependency: per-client-IP limit, with a separate counter per endpoint scope."""

    async def dependency(request: Request) -> None:
        s = request.app.state.settings
        await request.app.state.limiter.check([Limit(f"{scope}:ip:{_client_ip(request)}", getattr(s, setting))])

    return Depends(dependency)


async def _redeem_limits(request: Request, body: RedeemIn) -> None:
    s = request.app.state.settings
    await request.app.state.limiter.check(
        [
            Limit(f"redeem:customer:{body.customer_id}", s.rate_limit_redeem_per_customer),
            Limit(f"redeem:ip:{_client_ip(request)}", s.rate_limit_redeem_per_ip),
        ]
    )


limit_redeem = Depends(_redeem_limits)
