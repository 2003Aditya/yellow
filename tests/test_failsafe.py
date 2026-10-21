import psycopg

from tests.conftest import TEST_DB, assert_counter_consistent, redeem, seed


async def test_lock_timeout_returns_503_and_retry_succeeds(make_client, db):
    client = await make_client(lock_timeout_ms=300)
    await seed(client, "LOCKED", max_redemptions=5)

    blocker = await psycopg.AsyncConnection.connect(TEST_DB)
    await blocker.execute("SELECT 1 FROM coupons WHERE code = 'LOCKED' FOR UPDATE")
    try:
        resp = await redeem(client, "LOCKED", "c1", "o1", key="k-lock")
        assert resp.status_code == 503
        assert resp.json()["error"]["code"] == "RETRYABLE"
        assert resp.headers["Retry-After"]
    finally:
        await blocker.rollback()
        await blocker.close()

    # nothing was persisted — not even the idempotency key
    cur = await db.execute("SELECT count(*) FROM idempotency_keys WHERE key = 'k-lock'")
    assert (await cur.fetchone())[0] == 0
    assert await assert_counter_consistent(db, "LOCKED") == 0

    retry = await redeem(client, "LOCKED", "c1", "o1", key="k-lock")
    assert retry.status_code == 200 and "replayed" not in retry.json()


async def test_db_check_constraint_is_last_line_of_defence(client, db):
    await seed(client, "GUARD", max_redemptions=1)
    try:
        await db.execute("UPDATE coupons SET redeemed_count = 2 WHERE code = 'GUARD'")
        raise AssertionError("constraint should have fired")
    except psycopg.errors.CheckViolation:
        pass


async def test_health_endpoints(client):
    assert (await client.get("/healthz")).status_code == 200
    assert (await client.get("/readyz")).json()["db"] == "up"
