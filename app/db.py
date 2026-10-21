import asyncio
import functools
import logging
import random
from psycopg import errors
from psycopg_pool import AsyncConnectionPool

from app.config import Settings
from app.migrate import migrate

log = logging.getLogger("coupons.db")


def create_pool(settings: Settings, size: int | None = None, name: str = "main") -> AsyncConnectionPool:
    options = f"-c statement_timeout={settings.statement_timeout_ms} -c lock_timeout={settings.lock_timeout_ms}"
    app_name = f"coupons:{settings.instance_id}" if name == "main" else f"coupons-{name}:{settings.instance_id}"
    return AsyncConnectionPool(
        settings.database_url,
        min_size=min(settings.db_pool_min, size or settings.db_pool_max),
        max_size=size or settings.db_pool_max,
        timeout=settings.db_pool_timeout_s,
        name=name,
        kwargs={"options": options, "application_name": app_name[:63]},
        # Validate a connection before handing it out: after a DB crash/restart every pooled
        # connection is dead, and this makes the pool replace them instead of failing requests.
        check=AsyncConnectionPool.check_connection,
        open=False,
    )


async def open_pool(pool: AsyncConnectionPool, settings: Settings) -> None:
    """Open the pool, waiting (with backoff inside psycopg_pool) for Postgres to come up."""
    await pool.open(wait=True, timeout=settings.db_connect_timeout_s)
    async with pool.connection() as conn:
        applied = await migrate(conn)  # advisory-locked: safe with many instances booting together
    if applied:
        log.info("applied migrations: %s", ", ".join(applied))


def retry_on_conflict(fn):
    """Retry the whole transaction on deadlock / serialization failure. Safe because each
    attempt is a fresh transaction that either commits completely or not at all."""

    @functools.wraps(fn)
    async def wrapper(*args, attempts: int = 3, **kwargs):
        for attempt in range(1, attempts + 1):
            try:
                return await fn(*args, **kwargs)
            except (errors.DeadlockDetected, errors.SerializationFailure):
                if attempt == attempts:
                    raise
                log.warning("transaction conflict, retrying", extra={"attempt": attempt})
                await asyncio.sleep(random.uniform(0.005, 0.03) * attempt)

    return wrapper
