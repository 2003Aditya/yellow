-- 0001_init: core tables for coupon redemption.
-- Hand-written and owned by us; applied by app/migrate.py (tracked in schema_migrations).
-- Never edit an applied migration: add 0002_*.sql instead (the runner verifies checksums).

CREATE TABLE IF NOT EXISTS coupons (
    code             text PRIMARY KEY,
    type             text NOT NULL CHECK (type IN ('STANDARD', 'STACKABLE')),
    max_redemptions  integer NOT NULL CHECK (max_redemptions > 0),
    redeemed_count   integer NOT NULL DEFAULT 0,
    discount_percent numeric(5, 2) NOT NULL CHECK (discount_percent > 0 AND discount_percent <= 100),
    expires_at       timestamptz NOT NULL,
    version          integer NOT NULL DEFAULT 1,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    -- Last line of defence: even buggy app code cannot over-redeem or go negative.
    CONSTRAINT coupons_count_in_range CHECK (redeemed_count >= 0 AND redeemed_count <= max_redemptions)
);

CREATE TABLE IF NOT EXISTS redemptions (
    id           bigserial PRIMARY KEY,
    coupon_code  text NOT NULL REFERENCES coupons (code),
    coupon_type  text NOT NULL,
    customer_id  text NOT NULL,
    order_id     text NOT NULL,
    status       text NOT NULL CHECK (status IN ('ACTIVE', 'CANCELLED')),
    redeemed_at  timestamptz NOT NULL,
    cancelled_at timestamptz,
    CONSTRAINT redemptions_order_id_key UNIQUE (order_id)
);

-- STANDARD coupons: once per customer, ever (cancelled rows still count).
CREATE UNIQUE INDEX IF NOT EXISTS uq_standard_once_per_customer
    ON redemptions (coupon_code, customer_id)
    WHERE coupon_type = 'STANDARD';

CREATE INDEX IF NOT EXISTS ix_redemptions_coupon ON redemptions (coupon_code);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key             text PRIMARY KEY,
    request_hash    text NOT NULL,
    response_status integer NOT NULL,
    response_body   jsonb NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rate_limits (
    bucket       text PRIMARY KEY,
    window_start timestamptz NOT NULL,
    hits         integer NOT NULL
);
