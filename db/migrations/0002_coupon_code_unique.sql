-- 0002: make "one coupon per code" a database guarantee, independent of the app.
--
-- coupons.code is already the PRIMARY KEY (coupons_pkey: UNIQUE + NOT NULL). The app
-- normalises codes (trim + UPPER), but uniqueness must not depend on the app: anything
-- inserting 'save10' next to 'SAVE10' would otherwise create a second coupon.
--
-- 1. Only the canonical form may be stored, so the PK's uniqueness is case-insensitive.
ALTER TABLE coupons
    ADD CONSTRAINT coupons_code_canonical
    CHECK (code = upper(btrim(code)) AND code <> '');

-- 2. Explicit, named unique index on the case-folded code. Redundant with (1) + the PK by
--    design: it keeps codes unique even if the canonical-form rule is ever relaxed.
CREATE UNIQUE INDEX uq_coupons_code_ci ON coupons (upper(code));
