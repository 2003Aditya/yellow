from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from app.cache import MetadataCache
from app.db import retry_on_conflict
from app.errors import ApiError, ErrorCode
from app.models import CouponIn

# Create-only. ON CONFLICT DO NOTHING makes a concurrent duplicate wait for the first insert
# to commit and then see it, instead of failing with a raw unique violation.
CREATE_SQL = """
INSERT INTO coupons (code, type, max_redemptions, discount_percent, expires_at)
VALUES (%(code)s, %(type)s, %(max_redemptions)s, %(discount_percent)s, %(expires_at)s)
ON CONFLICT (code) DO NOTHING
RETURNING code
"""

# Same definition as the existing row? Then the create was a harmless re-send.
SAME_DEFINITION_SQL = """
SELECT 1 FROM coupons
WHERE code = %(code)s AND type = %(type)s AND max_redemptions = %(max_redemptions)s
  AND discount_percent = %(discount_percent)s::numeric(5, 2) AND expires_at = %(expires_at)s
"""

STATUS_SQL = """
SELECT code, version, redeemed_count, max_redemptions,
       max_redemptions - redeemed_count AS remaining,
       now() >= expires_at AS expired
FROM coupons WHERE code = %s
"""

META_SQL = "SELECT type, discount_percent, expires_at FROM coupons WHERE code = %s AND version = %s"


@retry_on_conflict
async def create_coupon(pool: AsyncConnectionPool, cache: MetadataCache, coupon: CouponIn) -> tuple[bool, dict]:
    """Create a coupon; a code can exist only once (enforced by the database).

    Returns (created, coupon_view). Re-sending the identical definition is an idempotent
    no-op (created=False); a different definition for an existing code is rejected.
    """
    params = coupon.model_dump(mode="python")
    params["type"] = coupon.type.value
    async with pool.connection() as conn:
        created = await (await conn.execute(CREATE_SQL, params)).fetchone() is not None
        if not created and await (await conn.execute(SAME_DEFINITION_SQL, params)).fetchone() is None:
            raise ApiError(ErrorCode.COUPON_ALREADY_EXISTS, f"Coupon {coupon.code} already exists")
    return created, await get_coupon(pool, cache, coupon.code)


async def get_coupon(pool: AsyncConnectionPool, cache: MetadataCache, code: str) -> dict:
    """Counters always come straight from Postgres; static metadata from a version-checked cache."""
    async with pool.connection() as conn:
        cur = conn.cursor(row_factory=dict_row)  # cursor-scoped: never mutate pooled connections
        status = await (await cur.execute(STATUS_SQL, (code,))).fetchone()
        if status is None:
            raise ApiError(ErrorCode.UNKNOWN_COUPON, f"Coupon {code} does not exist")
        meta = cache.get(code, status["version"])
        if meta is None:
            row = await (await cur.execute(META_SQL, (code, status["version"]))).fetchone()
            if row is None:  # updated between the two reads; fall back to a single consistent read
                row = await (
                    await cur.execute("SELECT type, discount_percent, expires_at FROM coupons WHERE code = %s", (code,))
                ).fetchone()
            else:
                cache.put(code, status["version"], row)
            meta = row
    return {
        "code": status["code"],
        "type": meta["type"],
        "max_redemptions": status["max_redemptions"],
        "redeemed_count": status["redeemed_count"],
        "remaining": status["remaining"],
        "discount_percent": float(meta["discount_percent"]),
        "expires_at": meta["expires_at"].isoformat(),
        "expired": status["expired"],
    }
