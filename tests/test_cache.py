from tests.conftest import redeem, seed


async def test_cache_hits_but_counters_stay_live(client):
    cache = client.app.state.cache
    await seed(client, "CACHE", max_redemptions=5, type="STACKABLE")
    await client.get("/coupons/CACHE")
    hits_before = cache.hits
    await redeem(client, "CACHE")
    body = (await client.get("/coupons/CACHE")).json()
    assert cache.hits == hits_before + 1
    assert body["redeemed_count"] == 1 and body["remaining"] == 4


async def test_version_check_beats_stale_cache(client, db):
    """Another instance updating the row (simulated by direct SQL) must never be masked by our cache."""
    await seed(client, "CACHE", discount=10)
    await client.get("/coupons/CACHE")
    await db.execute("UPDATE coupons SET discount_percent = 55, version = version + 1 WHERE code = 'CACHE'")
    assert (await client.get("/coupons/CACHE")).json()["discount_percent"] == 55
