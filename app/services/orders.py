from psycopg_pool import AsyncConnectionPool

from app.db import retry_on_conflict


@retry_on_conflict
async def cancel_order(pool: AsyncConnectionPool, order_id: str) -> dict:
    """Give back the coupon slot used by an order — exactly once, however often it's called."""
    async with pool.connection() as conn, conn.transaction():
        row = await (
            await conn.execute("SELECT coupon_code FROM redemptions WHERE order_id = %s", (order_id,))
        ).fetchone()
        if row is None:
            return {"success": True, "order_id": order_id, "released": False, "reason": "NO_COUPON_ON_ORDER"}
        code = row[0]

        # Same lock order as redeem (coupon row first, then redemption) -> no deadlocks.
        await conn.execute("SELECT 1 FROM coupons WHERE code = %s FOR UPDATE", (code,))

        # Conditional transition: a concurrent second cancel blocks on the row lock, then
        # re-evaluates status = 'ACTIVE' against the committed row and matches nothing.
        released = await (
            await conn.execute(
                "UPDATE redemptions SET status = 'CANCELLED', cancelled_at = now() "
                "WHERE order_id = %s AND status = 'ACTIVE' RETURNING id",
                (order_id,),
            )
        ).fetchone()
        if released is None:
            return {"success": True, "order_id": order_id, "code": code, "released": False, "reason": "ALREADY_CANCELLED"}

        remaining = await (
            await conn.execute(
                "UPDATE coupons SET redeemed_count = redeemed_count - 1 WHERE code = %s "
                "RETURNING max_redemptions - redeemed_count",
                (code,),
            )
        ).fetchone()
        return {"success": True, "order_id": order_id, "code": code, "released": True, "remaining": remaining[0]}
