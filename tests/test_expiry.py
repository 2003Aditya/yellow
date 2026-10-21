import asyncio

import pytest

from tests.conftest import redeem, seed


@pytest.mark.concurrency
async def test_burst_across_expiry_instant_is_consistent(client, db):
    """Hammer a coupon while it expires. Every outcome must be decided by one clock
    (the DB transaction timestamp): successes strictly before expires_at, rejections at/after."""
    await seed(client, "EDGE", max_redemptions=100_000, type="STACKABLE")
    await db.execute("UPDATE coupons SET expires_at = now() + interval '400 milliseconds' WHERE code = 'EDGE'")

    results = []

    async def worker(w):
        for i in range(400):
            r = await redeem(client, "EDGE", f"c{w}", f"o{w}-{i}")
            results.append(r)
            if r.status_code == 410:
                return

    await asyncio.gather(*[worker(w) for w in range(8)])

    ok = [r for r in results if r.status_code == 200]
    expired = [r for r in results if r.status_code == 410]
    assert ok and expired, "test must straddle the expiry instant"
    assert len(ok) + len(expired) == len(results)

    cur = await db.execute(
        "SELECT count(*) FILTER (WHERE r.redeemed_at >= c.expires_at), count(*) "
        "FROM redemptions r JOIN coupons c ON c.code = r.coupon_code WHERE c.code = 'EDGE'"
    )
    late, total = await cur.fetchone()
    assert late == 0, "a redemption was accepted at/after expires_at"
    assert total == len(ok)


async def test_expiry_boundary_is_exclusive(client, db):
    """valid iff txn_now < expires_at; the boundary itself counts as expired."""
    await seed(client, "NOW")
    cur = await db.execute("SELECT now() >= now()")  # rule evaluated against a single txn timestamp
    assert (await cur.fetchone())[0] is True
    await db.execute("UPDATE coupons SET expires_at = now() WHERE code = 'NOW'")
    resp = await redeem(client, "NOW")
    assert resp.status_code == 410
