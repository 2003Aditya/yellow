PY  := .venv/bin/python
PIP := .venv/bin/pip
UV  := .venv/bin/uvicorn

export DATABASE_URL      ?= postgresql://coupon:coupon@localhost:55432/coupons
export TEST_DATABASE_URL ?= postgresql://coupon:coupon@localhost:55432/coupons_test

.PHONY: help venv db-up db-down db-reset db-logs psql migrate migrate-status \
        run run-1 run-2 seed demo \
        test test-concurrency test-multi test-ratelimit test-crash test-all clean

help:
	@echo "setup:    make venv | db-up | migrate"
	@echo "run:      make run (port 8000) | run-1 (8001) | run-2 (8002) | seed | demo"
	@echo "test:     make test | test-concurrency | test-multi | test-ratelimit | test-crash | test-all"
	@echo "database: make psql | migrate-status | db-logs | db-down | db-reset"
	@echo "other:    make clean"

# ---------- setup
venv:
	python3 -m venv .venv
	$(PIP) install -r requirements.txt

db-up:
	docker compose up -d --wait db

db-down:
	docker compose down

db-reset:
	docker compose down -v
	docker compose up -d --wait db

db-logs:
	docker compose logs -f db

psql:
	docker compose exec db psql -U coupon -d coupons

migrate:
	$(PY) -m app.migrate up

migrate-status:
	$(PY) -m app.migrate status

# ---------- run
run:
	$(UV) app.main:app --host 127.0.0.1 --port 8000 --reload

# two instances on the same database: run each in its own terminal
run-1:
	INSTANCE_ID=app-1 $(UV) app.main:app --host 127.0.0.1 --port 8001

run-2:
	INSTANCE_ID=app-2 $(UV) app.main:app --host 127.0.0.1 --port 8002

seed:
	$(PY) scripts/demo.py seed http://127.0.0.1:8000

demo:
	$(PY) scripts/demo.py demo http://127.0.0.1:8000

# ---------- test (all against a real Postgres; start it with `make db-up` first)
test:
	$(PY) -m pytest -v

test-concurrency:
	$(PY) -m pytest -v -m concurrency

test-multi:
	$(PY) -m pytest -v -m multiinstance

test-ratelimit:
	$(PY) -m pytest -v tests/test_ratelimit.py

# kills the Postgres container mid-burst, then restarts it
test-crash:
	$(PY) -m pytest -v -s -m dbcrash

test-all:
	$(PY) -m pytest -v -s -m ""

clean:
	rm -rf .venv .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
