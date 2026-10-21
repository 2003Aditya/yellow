# Coupon Redemption Service

A coupon redemption API for e-commerce checkout, built with **FastAPI + PostgreSQL**.

**Postgres is the only source of truth.** Every rule is enforced inside a single database transaction, using row locks, `UNIQUE` / `CHECK` constraints and the database clock. Nothing depends on in-process state, so the guarantees hold across any number of app instances. The test suite proves this by running against **two separate app processes** that share one database.

Design details: [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 1. Run it

Requirements: Docker and Python 3.11+.

```bash
make venv          # create .venv and install requirements.txt
make db-up         # start Postgres 16 (Docker Compose, localhost:55432) and wait until healthy
make migrate       # apply db/migrations/*.sql  (the app also applies pending migrations on boot)
make run           # API on http://127.0.0.1:8000  (interactive docs at /docs)
```

In a second terminal:

```bash
make demo          # create coupons, redeem, retry, trigger each error, cancel twice
```

Two instances on the same database (one per terminal):

```bash
make run-1         # http://127.0.0.1:8001
make run-2         # http://127.0.0.1:8002
```

Tests (Postgres must be up: `make db-up`):

```bash
make test              # full suite (64 tests); excludes the crash test
make test-concurrency  # burst / race tests only
make test-multi        # tests that boot two app processes
make test-ratelimit    # rate-limit tests only
make test-crash        # SIGKILLs the Postgres container mid-burst, then restarts it
make test-all          # everything, including the crash test (65 tests)
```

Other commands: `make psql`, `make migrate-status`, `make db-logs`, `make db-down`, `make db-reset` (drops all data), `make clean`, `make help`.

Tests use a separate database, `coupons_test` (created by `db/init.sql`), so they never touch dev data. All settings come from environment variables; see `app/config.py`.

---

## 2. API

| Method | Path | Behaviour | Rate limit (per IP, per 10 s) |
|---|---|---|---|
| POST | `/coupons` | Create a coupon. **Create-only**: an existing code gives `409 COUPON_ALREADY_EXISTS`; re-sending the identical definition gives `200` with `created:false` | 20 |
| POST | `/redeem` | Redeem a coupon. Needs the `Idempotency-Key` header | 100 per IP **and** 10 per `customer_id` |
| POST | `/orders/{order_id}/cancel` | Gives the coupon slot back, exactly once | 60 |
| GET | `/coupons/{code}` | `redeemed_count`, `remaining`, `max_redemptions`, read live from Postgres | 300 |
| GET | `/healthz`, `/readyz` | Liveness / database readiness | 600 (shared) |

Success: `{"success": true, ...}`. Errors always use one shape: `{"success": false, "error": {"code", "message"}}`.

| Error code | HTTP | Meaning |
|---|---|---|
| `UNKNOWN_COUPON` | 404 | Code doesn't exist |
| `COUPON_EXPIRED` | 410 | Transaction time ≥ `expires_at` |
| `ALREADY_REDEEMED_BY_CUSTOMER` | 409 | STANDARD coupon already used by this customer |
| `ORDER_ALREADY_HAS_COUPON` | 409 | The order already has a coupon |
| `NO_REDEMPTIONS_LEFT` | 409 | Global cap reached |
| `IDEMPOTENCY_KEY_REQUIRED` | 400 | `Idempotency-Key` header missing |
| `IDEMPOTENCY_KEY_REUSED` | 422 | Same key, different request body |
| `COUPON_ALREADY_EXISTS` | 409 | A coupon with that code already exists |
| `RATE_LIMITED` | 429 | Over the limit; see `Retry-After` |
| `RETRYABLE` | 503 | Lock or statement timeout, coupon overloaded, or DB down. **Nothing was applied**, so retry with the same `Idempotency-Key` |
| `VALIDATION_ERROR` | 422 | Bad input |

Rules are always checked in this order, so the same request always gets the same answer: **UNKNOWN → EXPIRED → ALREADY_REDEEMED_BY_CUSTOMER → ORDER_ALREADY_HAS_COUPON → NO_REDEMPTIONS_LEFT**.

An idempotent replay returns the **original** status and body, plus `"replayed": true` and the header `Idempotent-Replayed: true`. Every response carries `X-Request-ID` and `X-Instance-ID` (which process served it).

---

## 3. SQL schema

Hand-written SQL, owned by this project; no ORM or framework generates it.

```
db/migrations/0001_init.sql                tables, constraints, indexes
db/migrations/0002_coupon_code_unique.sql  canonical-code CHECK + case-insensitive UNIQUE index
app/migrate.py                             runner: python -m app.migrate [up|status]
```

- Migrations run in filename order and are recorded in `schema_migrations` with a SHA-256 checksum. If an applied file is edited, the runner refuses to start.
- All pending migrations run in **one transaction** under an **advisory lock**, so instances booting at the same time can't apply them twice or half-apply them.

| Constraint / index | Guarantees |
|---|---|
| `coupons_pkey` (PRIMARY KEY on `code`) | A coupon code exists once |
| `coupons_code_canonical CHECK (code = upper(btrim(code)))` | Only the canonical spelling can be stored, so `save10` can't sit next to `SAVE10` |
| `uq_coupons_code_ci UNIQUE (upper(code))` | Codes are unique ignoring case, even if the canonical rule is ever relaxed |
| `coupons_count_in_range CHECK (0 <= redeemed_count <= max_redemptions)` | The counter can never over-redeem or go negative, even with buggy app code |
| `uq_standard_once_per_customer UNIQUE (coupon_code, customer_id) WHERE coupon_type='STANDARD'` | A STANDARD coupon is used once per customer, ever |
| `redemptions_order_id_key UNIQUE (order_id)` | One coupon per order |
| `status CHECK IN ('ACTIVE','CANCELLED')` | Cancel is a one-way state change |
| `idempotency_keys` PRIMARY KEY | One stored outcome per Idempotency-Key |
| `rate_limits` PRIMARY KEY | One counter row per rate-limit bucket |

---

## 4. How each rule is enforced

**Redeem** is one transaction with two phases:

1. **Claim the Idempotency-Key** with `INSERT … ON CONFLICT DO NOTHING`. If the same key is already in flight on any instance, this insert *waits* until that transaction finishes, then replays its stored answer.
2. **Phase 1, no lock.** Read the coupon and check: unknown, expired, STANDARD customer already used it, order already has a coupon, sold out. Each of these answers is final for this request, so it is returned immediately. Rejected requests **never queue on the hot row**, which matters most once a flash sale sells out.
3. **Phase 2, row lock.** `SELECT … FOR UPDATE` on the coupon. This serialises every grant for that coupon across all instances. The usage checks are then **re-run in a new statement**: in READ COMMITTED, a statement only sees rows committed before it started, so this is how it sees the redemption committed by the transaction it was queued behind. The count is re-checked under the lock.
4. Insert the redemption, increment the counter, and store the response under the key, all in the same commit. If anything fails, everything rolls back, including the key.

| Rule | Mechanism |
|---|---|
| Never more than `max_redemptions`, even under a burst | Row lock + count check under the lock + `CHECK` constraint backstop |
| STANDARD: once per customer, ever | Usage check after the lock + partial unique index backstop |
| STACKABLE: no per-customer limit, global cap applies | Not covered by the per-customer index; same lock and cap path |
| Expiry at the exact instant | Decided by Postgres `now()` (the transaction start time, on the DB's clock). Lock waits don't change it. `now() >= expires_at` counts as expired, and the same timestamp is stored as `redeemed_at` |
| Cancel returns the slot once | Lock the coupon, then `UPDATE … SET status='CANCELLED' WHERE order_id=? AND status='ACTIVE'`. The counter is decremented only if a row changed. Same lock order as redeem, so no deadlocks |
| Retries never double-redeem | The key and the redemption commit or roll back together |
| `GET` always exact | The counter changes in the same transaction as the redemption rows; counts are never cached |
| No duplicate coupons | PK + canonical `CHECK` + case-insensitive unique index. A concurrent duplicate create waits for the first to commit, then gets `409` |

**How requests queue and finish:**

| Mechanism | Purpose |
|---|---|
| **Bulkhead**: at most 8 in-flight redeem transactions per coupon per instance (`REDEEM_MAX_INFLIGHT_PER_COUPON`) | A hot coupon can't occupy the whole connection pool and starve other coupons. Waiting requests hold no connection and no lock. Waiting more than 3 s returns `503` |
| `lock_timeout` 2 s, `statement_timeout` 3 s, pool checkout timeout 5 s | Nothing waits forever. Every timeout returns `503 RETRYABLE` with nothing persisted |
| Deadlock / serialization failures | Retried in-server up to 3× with jitter |
| Postgres lock queue | First-come, first-served per coupon row |

**Rate limiting:** a fixed-window counter per bucket in the `rate_limits` table. It is updated by **one upsert statement**, with buckets in sorted order, so it never holds one limiter row's lock while waiting on another. It is shared by all instances and has **its own small pool**, so checkout traffic and the limiter can't starve each other. It **fails open** within 200 ms: the limiter protects capacity, not correctness. A 429 is never stored under the Idempotency-Key.

**Caching:** coupon metadata (type, discount, expiry) is cached in-process and checked against the row's `version` on every read, so a stale entry is never served. Counters are never cached, and `/redeem` never reads the cache.

**DB failure:** the service fails closed (`503`, never a guessed success). The pool validates connections on checkout, so it recovers by itself after a database restart.

---

## 5. Tests in detail

All **65 tests** run against a real Postgres. There are no mocks, because the guarantees live in the database. Every concurrency test also checks the invariant **`redeemed_count == number of ACTIVE redemption rows`** directly in the database (`assert_counter_consistent`).

There are three ways of creating concurrency:
- **In-process:** `asyncio.gather` firing requests at one app with a 30-connection pool, so Postgres sees real parallel transactions.
- **Two processes:** two `uvicorn` OS processes (`app-1`, `app-2`), each with its own memory and pool, sharing one database. Requests alternate between them over real HTTP.
- **Gate:** a separate session holds the coupon's row lock. The test waits until `pg_stat_activity` shows the app transactions **blocked inside Postgres at the same moment**, checks that every `pg_blocking_pids` chain leads back to the gate, and only then releases it. This makes "both requests hit the DB at the same time" certain rather than likely.

### 5.1 Two app processes, one database: `tests/test_multi_instance.py`
| Test | What it does | What it proves |
|---|---|---|
| `test_two_processes_are_really_separate` | Reads `X-Instance-ID` from both instances | Two distinct processes are serving |
| `test_burst_over_capacity_across_instances_is_exact` | 300 simultaneous redeems for 25 slots, alternating instances, distinct customers | Exactly 25 succeed and 275 get `NO_REDEMPTIONS_LEFT`. Both instances granted slots. Each `remaining` value 0–24 was handed out exactly once. Both instances report `25 / 0` |
| `test_same_idempotency_key_retried_is_charged_once` | (a) Original on app-1, then the same key retried on app-2 and app-1. (b) 20 racing requests with one key, split across both | Retries replay the identical original. Exactly one non-replayed response per key, one DB row per key, and the counter goes up by 1 per key |
| `test_cancel_twice_returns_slot_once` | (a) Cancel the same order on app-1, then on app-2. (b) 20 simultaneous cancels of another order across both. (c) 10 new redeems | The second cancel gives `ALREADY_CANCELLED`; each order frees one slot. Exactly 2 of the 10 new redeems fit into the 2 freed slots |

### 5.2 At the database: same-instant requests and DB failures: `tests/test_db_level.py`
| Test | Scenario | Result |
|---|---|---|
| `test_last_slot_two_instances_same_instant` | app-1 and app-2 both blocked inside Postgres on the **last** slot (gate) | Exactly one `200`, one `NO_REDEMPTIONS_LEFT` |
| `test_many_requests_all_inside_db_at_once` | 36 requests (18 per instance) for 5 slots while the row is locked | The bulkhead lets **exactly 8 per instance (16) into Postgres**; the other 20 wait in the app with no connection held. After release, exactly 5 succeed and the rest get `NO_REDEMPTIONS_LEFT` |
| `test_standard_same_customer_two_orders_same_instant` | STANDARD coupon: the same customer, two orders, two instances, same instant | One `200`. The other gets `ALREADY_REDEEMED_BY_CUSTOMER`, because the after-lock re-check saw the first commit |
| `test_rejections_never_wait_on_the_row_lock` | While the row lock is held: sold out, STANDARD already used, order already has a coupon, expired | All four are answered with the right code in **under 1 s** without waiting on the lock |
| `test_same_idempotency_key_same_instant_on_both_instances` | Original on app-1, retry on app-2, at the same time | The retry is shown blocked *by the original's transaction* on the key's unique index, then replays it; charged once |
| `test_double_cancel_same_instant_on_both_instances` | Two cancels of one order, one per instance, at the same time | Exactly one `released:true`; the counter is back to 0 |
| `test_db_constraints_hold_even_without_the_app` | Raw SQL with no app: two transactions push the count past the cap, and two insert the same STANDARD customer | The second transaction waits, then gets `CheckViolation` / `UniqueViolation`. The schema alone refuses |
| `test_session_killed_mid_transaction_rolls_back_cleanly` | `pg_terminate_backend` on app-1's session after it claimed the key, before commit | The client gets `503`. Key, redemption and counter are all rolled back. A retry with the same key succeeds, and app-1 keeps serving |
| `test_postgres_hard_crash_during_burst` (`make test-crash`) | **SIGKILL the Postgres container** while 400 redeems are in flight on both instances (cap 150), then restart it | While down, `/redeem` and `/readyz` return `503`. After WAL recovery, every acknowledged redemption is on disk, the counter matches the rows, and there are no half-written keys. Retrying the in-doubt requests with the same keys ends at exactly **150/150**, nothing double-charged |

Crash-test results so far (5 runs): about 18–26 acknowledged before the kill and about 375 in doubt each time. Every run ended at 150/150.

### 5.3 Bursts within one process: `tests/test_concurrency.py`
| Test | What it does | What it proves |
|---|---|---|
| `test_flash_sale_never_over_redeems` | 200 simultaneous redeems for 10 slots (STANDARD, distinct customers) | Exactly 10 succeed and 190 get `NO_REDEMPTIONS_LEFT`; `GET` shows `10 / 0` |
| `test_standard_same_customer_burst` | 50 simultaneous redeems by **one** customer, different orders, on a STANDARD coupon | Exactly 1 succeeds; 49 get `ALREADY_REDEEMED_BY_CUSTOMER` |
| `test_stackable_same_customer_burst_respects_global_cap` | 60 simultaneous redeems by one customer on a STACKABLE coupon with 25 slots | Exactly 25 succeed: no per-customer limit, but the cap holds |
| `test_get_is_exact_during_burst` | 40 redeems while `GET` polls 20 times | Every snapshot satisfies `redeemed_count + remaining == 40`; the final count is 40 |

### 5.4 Per-coupon bulkhead: `tests/test_bulkhead.py`
| Test | What it does | What it proves |
|---|---|---|
| `test_without_bulkhead_one_hot_coupon_starves_the_pool` | Pool of 6, cap set to 6 (no effective bulkhead). A hot coupon's lock is held and flooded with 12 requests; then one redeem for a *different* coupon | The other coupon **gets `503`**: it couldn't get a connection. This is the problem the bulkhead fixes |
| `test_with_bulkhead_other_coupons_keep_flowing` | Same flood with the cap at 2 | The other coupon is served in **under 0.5 s**. All 12 hot-coupon requests complete once the lock is released |
| `test_bulkhead_queue_timeout_is_retryable_and_persists_nothing` | Cap 1, queue timeout 0.3 s, lock held | The queued request gets `503 RETRYABLE` with `Retry-After: 1`. Its key isn't stored, and a retry with the same key succeeds |

### 5.5 Rate limiting: `tests/test_ratelimit.py`
| Test | What it does | What it proves |
|---|---|---|
| `test_every_endpoint_is_rate_limited[rate_limit_coupon_create_per_ip]` | Limit 3; 3 × `POST /coupons`, then a 4th | The 4th gets `429 RATE_LIMITED` with `Retry-After` |
| `test_every_endpoint_is_rate_limited[rate_limit_coupon_read_per_ip]` | Same for `GET /coupons/{code}` | 429 on the 4th |
| `test_every_endpoint_is_rate_limited[rate_limit_redeem_per_ip]` | Same for `POST /redeem` (per IP) | 429 on the 4th |
| `test_every_endpoint_is_rate_limited[rate_limit_cancel_per_ip]` | Same for `POST /orders/{id}/cancel` | 429 on the 4th |
| `test_every_endpoint_is_rate_limited[rate_limit_health_per_ip]` | Same for `/healthz` and `/readyz` (alternating) | 429 on the 4th |
| `test_endpoints_have_independent_counters` | Use up the `GET /coupons` budget | Reads get 429, but redeem and healthz still succeed: separate counters per endpoint |
| `test_per_customer_limit_then_retry_same_key` | Per-customer limit 3: customer `c1` redeems 4 times | The 4th gets 429, and customer `c2` is unaffected. After the window passes, the **same key** succeeds: a 429 is never stored under the key |
| `test_limiter_fails_open` | The limiter's table is made unavailable | Redeems still succeed; a limiter outage doesn't stop checkout |
| `test_limiter_uses_its_own_pool` | Inspects app state | The limiter's pool is separate from the checkout pool |

### 5.6 Redeem rules and errors: `tests/test_redeem.py`
| Test | What it proves |
|---|---|
| `test_success_and_remaining` | A successful redeem returns `remaining` and the discount; `GET` shows the new count. Codes are case-insensitive |
| `test_unknown_code` | `404 UNKNOWN_COUPON` |
| `test_expired` | `410 COUPON_EXPIRED` for an already-expired coupon |
| `test_standard_once_per_customer` | The same customer's second redeem gets `ALREADY_REDEEMED_BY_CUSTOMER`; a different customer succeeds |
| `test_standard_once_ever_even_after_cancel` | After cancelling, the slot is freed, but the same customer still can't reuse a STANDARD code |
| `test_stackable_no_per_customer_limit` | One customer redeems a STACKABLE coupon up to the cap, then gets `NO_REDEMPTIONS_LEFT` |
| `test_no_redemptions_left` | `409 NO_REDEMPTIONS_LEFT` once the cap is reached |
| `test_order_cannot_take_two_coupons` | A second coupon on the same order gets `ORDER_ALREADY_HAS_COUPON` |
| `test_error_precedence_expired_beats_exhausted` | Sold out *and* expired gives `COUPON_EXPIRED` |
| `test_standard_precedence_already_used_beats_sold_out` | A STANDARD customer who already used the coupon always gets `ALREADY_REDEEMED_BY_CUSTOMER`, before and after it sells out; a new customer gets `NO_REDEMPTIONS_LEFT` |
| `test_same_order_new_key_is_deterministic` | Resending an order with a new key never creates a second redemption: `ORDER_ALREADY_HAS_COUPON` |
| `test_missing_idempotency_key` | `400 IDEMPOTENCY_KEY_REQUIRED` |
| `test_validation` | Empty `customer_id` gives `422` |

### 5.7 Idempotency: `tests/test_idempotency.py`
| Test | What it proves |
|---|---|
| `test_retry_replays_original_response` | A retry returns the identical body with `replayed:true` and the `Idempotent-Replayed` header; the counter is 1 |
| `test_business_errors_are_replayed_too` | A `NO_REDEMPTIONS_LEFT` answer is stored and replayed with the same `409` |
| `test_same_key_different_body_rejected` | Reusing a key with a different body gives `422 IDEMPOTENCY_KEY_REUSED` |
| `test_concurrent_retries_same_key_redeem_once` | 50 simultaneous requests with one key: 1 redemption, 49 replayed, all with the same `redeemed_at` |

### 5.8 Cancellation: `tests/test_cancel.py`
| Test | What it proves |
|---|---|
| `test_cancel_releases_slot_once` | The first cancel gives `released:true` and `remaining` goes back up; the second gives `ALREADY_CANCELLED`. The freed slot can be redeemed by someone else |
| `test_cancel_order_without_coupon` | Cancelling an order with no coupon is a `200` no-op (`NO_COUPON_ON_ORDER`) |
| `test_concurrent_cancels_release_exactly_once` | 20 simultaneous cancels of one order: exactly 1 releases; the counter drops by exactly 1 |
| `test_cancels_and_redeems_interleaved` | 10 cancels and 30 new redeems running at the same time on a full coupon: the counter matches the rows and never exceeds the cap |

### 5.9 Expiry: `tests/test_expiry.py`
| Test | What it proves |
|---|---|
| `test_burst_across_expiry_instant_is_consistent` | 8 workers redeem continuously while the coupon expires 400 ms in. Both successes and `COUPON_EXPIRED` occur, and **no stored redemption has `redeemed_at >= expires_at`** |
| `test_expiry_boundary_is_exclusive` | `expires_at = now()` counts as expired (the boundary is exclusive) |

### 5.10 Coupon creation and duplicates: `tests/test_coupons.py`
| Test | What it proves |
|---|---|
| `test_create_then_get` | Create gives `201`; `GET` shows `0` used and `max` remaining |
| `test_code_is_case_insensitive` | `"  Mixed10 "` is stored as `MIXED10` and found as `mixed10` |
| `test_duplicate_code_with_different_definition_is_rejected` | A different max, discount or type for an existing code gives `409 COUPON_ALREADY_EXISTS`; the original row is unchanged and there is no second row |
| `test_identical_resend_is_idempotent` | Re-sending the identical definition gives `200`, `created:false` |
| `test_concurrent_creates_of_same_code_yield_exactly_one_coupon` | 30 simultaneous creates of one code with different definitions: exactly one `201`, 29 `409`, one row |
| `test_database_rejects_duplicate_codes_on_its_own` | Raw SQL with no app: a duplicate `SAVE10` raises `UniqueViolation`; `save10` and `" SAVE10"` raise `CheckViolation` |
| `test_validation_errors` | `max_redemptions` 0, discount 101, a timezone-less `expires_at`, an unknown type and a blank code all give `422` |
| `test_get_unknown` | `404 UNKNOWN_COUPON` |

### 5.11 Fail-safety: `tests/test_failsafe.py`
| Test | What it proves |
|---|---|
| `test_lock_timeout_returns_503_and_retry_succeeds` | A request blocked longer than `lock_timeout` gets `503 RETRYABLE` with `Retry-After`. Nothing is persisted (not even the key), and the retry with the same key succeeds |
| `test_db_check_constraint_is_last_line_of_defence` | A direct SQL update pushing the count above the max is rejected by the `CHECK` constraint |
| `test_health_endpoints` | `/healthz` gives `200`; `/readyz` reports `db: up` |

### 5.12 Cache: `tests/test_cache.py`
| Test | What it proves |
|---|---|
| `test_cache_hits_but_counters_stay_live` | Metadata is served from the cache (hit counter rises) while `redeemed_count` / `remaining` reflect the latest redeem |
| `test_version_check_beats_stale_cache` | When the row is changed directly in the database (simulating another instance), the version check makes the next `GET` return the new value |

### 5.13 Proving the test catches in-memory locking
To confirm the two-process test catches the failure the brief warns about, I temporarily replaced the `FOR UPDATE` row lock with an in-process `asyncio.Lock`. `test_burst_over_capacity_across_instances_is_exact` then **failed**: the two processes didn't coordinate, and only the `CHECK` constraint stopped the over-redemption, surfacing it as errors. With the real code it passes.

---

## 6. Assumptions
- **"Once per customer, ever"** is read strictly: cancelling a STANDARD redemption frees the global slot but doesn't let that customer reuse the code. To allow reuse, add a migration changing the index predicate to `WHERE coupon_type='STANDARD' AND status='ACTIVE'`.
- One coupon per order.
- Coupons are create-only; there is no update endpoint.
- Cancelling an order whose coupon has since expired still frees the slot, which keeps the count accurate.
- Idempotency keys and rate-limit rows are kept indefinitely. In production, a TTL purge job would clean them up.

## 7. Layout
```
db/migrations/        0001_init.sql, 0002_coupon_code_unique.sql
db/init.sql           creates the coupons_test database
app/main.py           app factory, middleware, error handlers, JSON logging
app/routes.py         HTTP layer (+ rate-limit dependency on every route)
app/services/         coupons (create/get) · redemption (redeem + idempotency) · orders (cancel)
app/db.py             pools, deadlock retry       app/migrate.py   migration runner
app/ratelimit.py      Postgres upsert limiter     app/bulkhead.py  per-coupon concurrency cap
app/cache.py          version-checked metadata cache
tests/                65 tests (see section 5)
scripts/demo.py       make seed / make demo
```
