import pytest

from tests.conftest import iso_in, redeem, seed

ENDPOINTS = {
    # scope setting                      -> request factory
    "rate_limit_coupon_create_per_ip": lambda c, i: c.post(
        "/coupons", json={"code": f"RL{i}", "max_redemptions": 1, "discount_percent": 5,
                          "expires_at": iso_in(60), "type": "STANDARD"}),
    "rate_limit_coupon_read_per_ip": lambda c, i: c.get("/coupons/RLX"),
    "rate_limit_redeem_per_ip": lambda c, i: redeem(c, "RLX", f"cust-{i}"),
    "rate_limit_cancel_per_ip": lambda c, i: c.post(f"/orders/o{i}/cancel"),
    "rate_limit_health_per_ip": lambda c, i: c.get("/healthz" if i % 2 else "/readyz"),
}


@pytest.mark.parametrize("setting", list(ENDPOINTS))
async def test_every_endpoint_is_rate_limited(make_client, db, setting):
    client = await make_client(rate_limit_enabled=True, rate_limit_redeem_per_customer=1000, **{setting: 3})
    if setting in ("rate_limit_coupon_read_per_ip", "rate_limit_redeem_per_ip"):
        await db.execute("INSERT INTO coupons (code, type, max_redemptions, discount_percent, expires_at) "
                         "VALUES ('RLX', 'STACKABLE', 100, 5, now() + interval '1 hour')")
    send = ENDPOINTS[setting]
    statuses = [(await send(client, i)).status_code for i in range(3)]
    assert 429 not in statuses
    limited = await send(client, 99)
    assert limited.status_code == 429, setting
    assert limited.json()["error"]["code"] == "RATE_LIMITED"
    assert int(limited.headers["Retry-After"]) >= 1


async def test_endpoints_have_independent_counters(make_client, db):
    client = await make_client(rate_limit_enabled=True, rate_limit_coupon_read_per_ip=2)
    await seed(client, "IND")
    for _ in range(2):
        await client.get("/coupons/IND")
    assert (await client.get("/coupons/IND")).status_code == 429
    assert (await redeem(client, "IND", "c1")).status_code == 200  # redeem budget is separate
    assert (await client.get("/healthz")).status_code == 200


async def test_per_customer_limit_then_retry_same_key(make_client, db):
    client = await make_client(rate_limit_enabled=True, rate_limit_redeem_per_customer=3, rate_limit_window_s=1)
    await seed(client, "RL", max_redemptions=100, type="STACKABLE")
    for i in range(3):
        assert (await redeem(client, "RL", "c1", f"o{i}")).status_code == 200

    limited = await redeem(client, "RL", "c1", "o-x", key="retry-me")
    assert limited.status_code == 429
    assert limited.json()["error"]["code"] == "RATE_LIMITED"

    # other customers are unaffected
    assert (await redeem(client, "RL", "c2", "o-y")).status_code == 200

    # 429 was not stored under the key: once the window passes, the same key works
    await db.execute("UPDATE rate_limits SET window_start = now() - interval '5 seconds'")
    assert (await redeem(client, "RL", "c1", "o-x", key="retry-me")).status_code == 200


async def test_limiter_fails_open(make_client, db):
    client = await make_client(rate_limit_enabled=True, rate_limit_redeem_per_customer=1)
    await seed(client, "FO", type="STACKABLE")
    await db.execute("ALTER TABLE rate_limits RENAME TO rate_limits_offline")  # simulate limiter outage
    try:
        assert (await redeem(client, "FO", "c1")).status_code == 200
        assert (await redeem(client, "FO", "c1")).status_code == 200
    finally:
        await db.execute("ALTER TABLE rate_limits_offline RENAME TO rate_limits")


async def test_limiter_uses_its_own_pool(client):
    assert client.app.state.limiter.pool is not client.app.state.pool
