"""Database-level proofs: what Postgres itself does when requests collide or the DB fails.

Part 1: SIMULTANEOUS ARRIVAL. A burst only makes collisions *likely*; these tests make them
certain. A gate session holds the coupon row lock, requests from BOTH app processes are sent,
and we wait until pg_stat_activity shows every one of them blocked inside Postgres on that
lock. Then we open the gate and check the outcome.

Part 2: DB FAILURES. A session killed mid-transaction, and a hard SIGKILL of the Postgres
server during a burst (opt-in: `make test-crash`).
"""
import asyncio
import subprocess
import time
import uuid

import httpx
import psycopg
import pytest

from tests.conftest import ROOT, TEST_DB, assert_counter_consistent, iso_in

pytestmark = [pytest.mark.multiinstance, pytest.mark.concurrency]


# ----------------------------------------------------------------------------- helpers

def _code(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:6]}".upper()


async def _seed(http, url, code, max_redemptions, type="STANDARD"):
    resp = await http.post(
        f"{url}/coupons",
        json={"code": code, "max_redemptions": max_redemptions, "discount_percent": 20,
              "expires_at": iso_in(3600), "type": type},
    )
    assert resp.status_code == 201, resp.text


async def _redeem(http, url, code, customer, order, key):
    return await http.post(
        f"{url}/redeem",
        json={"code": code, "customer_id": customer, "order_id": order},
        headers={"Idempotency-Key": key},
    )


async def _connect():
    return await psycopg.AsyncConnection.connect(TEST_DB, autocommit=True)


class Gate:
    """Holds `SELECT ... FOR UPDATE` on a coupon row so app transactions pile up behind it."""

    def __init__(self, code):
        self.code = code

    async def __aenter__(self):
        self.conn = await psycopg.AsyncConnection.connect(TEST_DB)
        await self.conn.execute("SELECT 1 FROM coupons WHERE code = %s FOR UPDATE", (self.code,))
        cur = await self.conn.execute("SELECT pg_backend_pid()")
        self.pid = (await cur.fetchone())[0]
        return self

    async def open(self):
        await self.conn.rollback()

    async def __aexit__(self, *exc):
        await self.conn.close()


async def wait_for_blocked(db, n, timeout=6.0):
    """Wait until n app sessions are blocked on a lock inside Postgres; return their details."""
    deadline = time.monotonic() + timeout
    while True:
        cur = await db.execute(
            """SELECT pid, application_name, wait_event_type, wait_event, pg_blocking_pids(pid)
               FROM pg_stat_activity
               WHERE application_name LIKE 'coupons:app-%%' AND wait_event_type = 'Lock'
               ORDER BY application_name"""
        )
        rows = await cur.fetchall()
        if len(rows) >= n:
            return rows
        if time.monotonic() > deadline:
            raise AssertionError(f"expected {n} blocked sessions, saw {len(rows)}: {rows}")
        await asyncio.sleep(0.02)


def assert_queued_behind(gate, blocked):
    """Postgres row-lock queue: the first waiter is blocked by the gate, later waiters by the
    waiter ahead of them. Every chain must lead back to the gate's row lock."""
    pids = {r[0] for r in blocked}
    assert any(gate.pid in r[4] for r in blocked), "someone must wait directly on the gate"
    for pid, _, _, _, blockers in blocked:
        assert blockers and set(blockers) <= pids | {gate.pid}, f"{pid} blocked by something else: {blockers}"


# ----------------------------------------------- part 1: requests at the DB at the same time

async def test_last_slot_two_instances_same_instant(instances, http, db):
    """Two checkouts from two processes race for the LAST slot while both sit inside Postgres."""
    code = _code("LAST")
    await _seed(http, instances[0], code, max_redemptions=1)

    async with Gate(code) as gate:
        a = asyncio.create_task(_redeem(http, instances[0], code, "alice", "o-a", "k-a"))
        b = asyncio.create_task(_redeem(http, instances[1], code, "bob", "o-b", "k-b"))

        blocked = await wait_for_blocked(db, 2)
        assert {r[1] for r in blocked} == {"coupons:app-1", "coupons:app-2"}, "one session per process"
        assert_queued_behind(gate, blocked)

        await gate.open()
        ra, rb = await a, await b

    assert sorted([ra.status_code, rb.status_code]) == [200, 409]
    loser = ra if ra.status_code == 409 else rb
    assert loser.json()["error"]["code"] == "NO_REDEMPTIONS_LEFT"
    assert await assert_counter_consistent(db, code) == 1


async def test_many_requests_all_inside_db_at_once(instances, http, db):
    """36 requests (18 per process) for 5 slots while the row is locked. The per-coupon bulkhead
    (8 per instance) lets exactly 16 transactions into Postgres; the other 20 wait in the app
    holding no connection and no lock. When the lock is released everyone completes."""
    code = _code("PILE")
    await _seed(http, instances[0], code, max_redemptions=5)

    async with Gate(code) as gate:
        tasks = [
            asyncio.create_task(_redeem(http, instances[i % 2], code, f"c{i}", f"o{i}", f"k{i}"))
            for i in range(36)
        ]
        blocked = await wait_for_blocked(db, 16)
        await asyncio.sleep(0.3)  # give any extra request time to (wrongly) reach the DB
        blocked = await wait_for_blocked(db, 16)
        per_instance = {name: sum(r[1] == name for r in blocked) for name in ("coupons:app-1", "coupons:app-2")}
        assert per_instance == {"coupons:app-1": 8, "coupons:app-2": 8}, per_instance
        assert_queued_behind(gate, blocked)
        await gate.open()
        responses = await asyncio.gather(*tasks)

    assert sum(r.status_code == 200 for r in responses) == 5
    assert {r.json()["error"]["code"] for r in responses if r.status_code != 200} == {"NO_REDEMPTIONS_LEFT"}
    assert await assert_counter_consistent(db, code) == 5


async def test_standard_same_customer_two_orders_same_instant(instances, http, db):
    """STANDARD: the same customer checks out two orders at the same moment on two processes."""
    code = _code("STDTWIN")
    await _seed(http, instances[0], code, max_redemptions=10, type="STANDARD")

    async with Gate(code) as gate:
        a = asyncio.create_task(_redeem(http, instances[0], code, "alice", "order-a", "ka"))
        b = asyncio.create_task(_redeem(http, instances[1], code, "alice", "order-b", "kb"))
        blocked = await wait_for_blocked(db, 2)
        assert {r[1] for r in blocked} == {"coupons:app-1", "coupons:app-2"}
        await gate.open()
        ra, rb = await a, await b

    assert sorted([ra.status_code, rb.status_code]) == [200, 409]
    loser = ra if ra.status_code == 409 else rb
    # the loser re-checked usage AFTER acquiring the lock, so it reports the real reason
    assert loser.json()["error"]["code"] == "ALREADY_REDEEMED_BY_CUSTOMER"
    assert await assert_counter_consistent(db, code) == 1


async def test_rejections_never_wait_on_the_row_lock(instances, http, db):
    """While the coupon row is locked, requests that will be rejected are answered immediately:
    sold out, STANDARD customer already used it, order already has a coupon, expired."""
    code = _code("FAST")
    await _seed(http, instances[0], code, max_redemptions=1, type="STANDARD")
    assert (await _redeem(http, instances[0], code, "alice", "o1", "k1")).status_code == 200  # sold out now
    old = _code("OLD")
    await _seed(http, instances[0], old, max_redemptions=5)
    await db.execute("UPDATE coupons SET expires_at = now() - interval '1 second' WHERE code = %s", (old,))

    async with Gate(code), Gate(old):
        started = time.monotonic()
        results = await asyncio.gather(
            _redeem(http, instances[1], code, "bob", "o2", "k2"),      # sold out
            _redeem(http, instances[0], code, "alice", "o3", "k3"),    # STANDARD, already used
            _redeem(http, instances[1], code, "carol", "o1", "k4"),    # order already has a coupon
            _redeem(http, instances[0], old, "dave", "o5", "k5"),      # expired
        )
        elapsed = time.monotonic() - started
    assert [r.json()["error"]["code"] for r in results] == [
        "NO_REDEMPTIONS_LEFT", "ALREADY_REDEEMED_BY_CUSTOMER", "ORDER_ALREADY_HAS_COUPON", "COUPON_EXPIRED",
    ]
    assert elapsed < 1.0, f"rejections waited on the lock ({elapsed:.2f}s)"


async def test_same_idempotency_key_same_instant_on_both_instances(instances, http, db):
    """Original and retry hit different processes at the same moment."""
    code = _code("SAMEKEY")
    await _seed(http, instances[0], code, max_redemptions=10)

    async with Gate(code) as gate:
        first = asyncio.create_task(_redeem(http, instances[0], code, "alice", "o1", "dup-key"))
        await wait_for_blocked(db, 1)  # app-1 has claimed the key (uncommitted), waits on coupon lock
        retry = asyncio.create_task(_redeem(http, instances[1], code, "alice", "o1", "dup-key"))
        blocked = await wait_for_blocked(db, 2)
        # app-2 is NOT waiting on the coupon: it is waiting on app-1's uncommitted idempotency key
        app2 = next(r for r in blocked if r[1] == "coupons:app-2")
        app1 = next(r for r in blocked if r[1] == "coupons:app-1")
        assert app1[0] in app2[4], "retry is blocked by the original's transaction"
        await gate.open()
        r1, r2 = await first, await retry

    assert r1.status_code == r2.status_code == 200
    assert "replayed" not in r1.json() and r2.json()["replayed"] is True
    assert await assert_counter_consistent(db, code) == 1


async def test_double_cancel_same_instant_on_both_instances(instances, http, db):
    code = _code("CXL")
    await _seed(http, instances[0], code, max_redemptions=2)
    assert (await _redeem(http, instances[0], code, "c1", "order-x", "k1")).status_code == 200

    async with Gate(code) as gate:
        a = asyncio.create_task(http.post(f"{instances[0]}/orders/order-x/cancel"))
        b = asyncio.create_task(http.post(f"{instances[1]}/orders/order-x/cancel"))
        blocked = await wait_for_blocked(db, 2)
        assert {r[1] for r in blocked} == {"coupons:app-1", "coupons:app-2"}
        await gate.open()
        ra, rb = await a, await b

    assert sorted([ra.json()["released"], rb.json()["released"]]) == [False, True]
    assert await assert_counter_consistent(db, code) == 0


async def test_db_constraints_hold_even_without_the_app(db):
    """Raw SQL with no app and no row lock: two concurrent transactions vs. the constraints."""
    await db.execute(
        "INSERT INTO coupons (code, type, max_redemptions, redeemed_count, discount_percent, expires_at) "
        "VALUES ('RAW', 'STANDARD', 1, 0, 10, now() + interval '1 hour')"
    )
    t1, t2 = await psycopg.AsyncConnection.connect(TEST_DB), await psycopg.AsyncConnection.connect(TEST_DB)
    try:
        # (a) over-cap: both increment a count of 0 with max 1
        await t1.execute("UPDATE coupons SET redeemed_count = redeemed_count + 1 WHERE code = 'RAW'")
        second = asyncio.create_task(
            t2.execute("UPDATE coupons SET redeemed_count = redeemed_count + 1 WHERE code = 'RAW'")
        )
        await asyncio.sleep(0.2)
        assert not second.done(), "t2 must wait for t1's row lock"
        await t1.commit()
        with pytest.raises(psycopg.errors.CheckViolation):
            await second
        await t2.rollback()

        # (b) same STANDARD customer inserted by two transactions at once
        sql = ("INSERT INTO redemptions (coupon_code, coupon_type, customer_id, order_id, status, redeemed_at) "
               "VALUES ('RAW', 'STANDARD', 'cust', %s, 'ACTIVE', now())")
        await t1.execute(sql, ("raw-1",))
        second = asyncio.create_task(t2.execute(sql, ("raw-2",)))
        await asyncio.sleep(0.2)
        assert not second.done(), "t2 must wait on t1's uncommitted unique-index entry"
        await t1.commit()
        with pytest.raises(psycopg.errors.UniqueViolation):
            await second
        await t2.rollback()
    finally:
        await t1.close()
        await t2.close()
    cur = await db.execute("SELECT redeemed_count FROM coupons WHERE code = 'RAW'")
    assert (await cur.fetchone())[0] == 1


# ----------------------------------------------------------------------- part 2: DB failures

async def test_session_killed_mid_transaction_rolls_back_cleanly(instances, http, db):
    """The app's DB session dies after it has claimed the idempotency key but before commit."""
    code = _code("KILL")
    await _seed(http, instances[0], code, max_redemptions=5)

    async with Gate(code) as gate:
        req = asyncio.create_task(_redeem(http, instances[0], code, "alice", "o1", "kill-key"))
        (victim_pid, app_name, *_), = await wait_for_blocked(db, 1)
        assert app_name == "coupons:app-1"
        await db.execute("SELECT pg_terminate_backend(%s)", (victim_pid,))  # simulated crash of that session
        resp = await req
        await gate.open()

    assert resp.status_code == 503 and resp.json()["error"]["code"] == "RETRYABLE"
    cur = await db.execute("SELECT count(*) FROM idempotency_keys WHERE key = 'kill-key'")
    assert (await cur.fetchone())[0] == 0, "uncommitted key claim must vanish with the transaction"
    assert await assert_counter_consistent(db, code) == 0

    retry = await _redeem(http, instances[1], code, "alice", "o1", "kill-key")
    assert retry.status_code == 200 and "replayed" not in retry.json()
    # the instance whose connection died keeps serving (the pool replaced the dead connection)
    assert (await _redeem(http, instances[0], code, "bob", "o2", "after-kill")).status_code == 200
    assert await assert_counter_consistent(db, code) == 2


def _docker(*args) -> str:
    return subprocess.run(["docker", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()


async def _wait_db_up(timeout=60.0):
    deadline = time.monotonic() + timeout
    while True:
        try:
            conn = await _connect()
            await conn.execute("SELECT 1")
            return conn
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            await asyncio.sleep(0.3)


@pytest.mark.dbcrash
async def test_postgres_hard_crash_during_burst(instances, http, db):
    """SIGKILL the Postgres server while ~400 redeems are in flight on two processes.

    Proves: fail closed while down; no torn writes; every acknowledged redemption survives;
    the counter still equals the rows; retrying with the same keys finishes the job exactly
    (including requests whose COMMIT reached disk but whose response was lost)."""
    code, cap, n = _code("CRASH"), 150, 400
    await _seed(http, instances[0], code, max_redemptions=cap)
    container = _docker("compose", "ps", "-q", "db")
    keys = [f"crash-{i}" for i in range(n)]

    async def attempt(i, url):
        try:
            return await _redeem(http, url, code, f"cust-{i}", f"ord-{i}", keys[i])
        except httpx.HTTPError:
            return None

    burst = asyncio.gather(*[attempt(i, instances[i % 2]) for i in range(n)])

    # crash once the burst is genuinely mid-flight
    while True:
        cur = await db.execute("SELECT redeemed_count FROM coupons WHERE code = %s", (code,))
        if (await cur.fetchone())[0] >= 10:
            break
        await asyncio.sleep(0.005)
    _docker("kill", "--signal", "KILL", container)
    try:
        first = await burst

        # --- while the DB is down: fail closed, report not-ready
        down = await _redeem(http, instances[0], code, "late", "ord-late", "late-key")
        assert down.status_code == 503 and down.json()["error"]["code"] == "RETRYABLE"
        ready = [(await http.get(f"{u}/readyz")).status_code for u in instances]
        assert ready == [503, 503]
    finally:
        # --- restart no matter what (Postgres runs WAL crash recovery on boot)
        _docker("start", container)
    db2 = await _wait_db_up()
    try:
        for url in instances:
            deadline = time.monotonic() + 30
            while (await http.get(f"{url}/readyz")).status_code != 200:
                assert time.monotonic() < deadline, f"{url} did not recover"
                await asyncio.sleep(0.3)

        acked = {f"ord-{i}" for i, r in enumerate(first) if r is not None and r.status_code == 200}
        unanswered = [i for i, r in enumerate(first) if r is None or r.status_code == 503]
        assert acked and unanswered, "the crash must land mid-burst"

        # durability: everything we told a client "success" is on disk
        cur = await db2.execute("SELECT order_id FROM redemptions WHERE coupon_code = %s", (code,))
        on_disk = {r[0] for r in await cur.fetchall()}
        assert acked <= on_disk, "an acknowledged redemption was lost"

        # atomicity: no half-applied transactions survived the crash
        assert await assert_counter_consistent(db2, code) == len(on_disk) <= cap
        cur = await db2.execute(
            """SELECT count(*) FROM redemptions r
               LEFT JOIN idempotency_keys k ON k.response_body->>'order_id' = r.order_id
               WHERE r.coupon_code = %s AND (k.key IS NULL OR k.response_status <> 200)""",
            (code,),
        )
        assert (await cur.fetchone())[0] == 0, "every redemption has its committed idempotency record"
        cur = await db2.execute("SELECT count(*) FROM idempotency_keys WHERE response_status = 0")
        assert (await cur.fetchone())[0] == 0, "no placeholder (in-flight) keys survived"

        # recovery: clients retry everything that got no definitive answer, with the SAME keys
        retries = await asyncio.gather(*[attempt(i, instances[(i + 1) % 2]) for i in unanswered])
        assert all(r is not None and r.status_code in (200, 409) for r in retries)
        ambiguous = sum(1 for r in retries if r.status_code == 200 and r.json().get("replayed"))

        final = await assert_counter_consistent(db2, code)
        assert final == cap, "after retries the coupon is used exactly to its cap"
        all_ok = len(acked) + sum(1 for r in retries if r.status_code == 200)
        assert all_ok == cap, "one redemption per successful key: nothing double-charged"
        print(f"\nacked before crash={len(acked)} in-doubt={len(unanswered)} "
              f"committed-but-unacknowledged (replayed on retry)={ambiguous} final={final}/{cap}")
    finally:
        await db2.close()
