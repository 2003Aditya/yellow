"""Minimal, hand-rolled SQL migration runner (no ORM / framework).

* Migrations are plain files: db/migrations/NNNN_description.sql, applied in filename order.
* Each applied migration is recorded in schema_migrations with a SHA-256 checksum; editing a
  migration after it has been applied is refused, so every environment runs identical DDL.
* All pending migrations run in ONE transaction (Postgres DDL is transactional) under an
  advisory lock, so several app instances starting at once can't race or half-apply.

usage: python -m app.migrate [up|status]
"""
import asyncio
import hashlib
import os
import sys
from pathlib import Path

import psycopg

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "db" / "migrations"
LOCK_ID = 724001


def _migrations() -> list[tuple[str, str, str]]:
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    return [(f.stem, f.read_text(), hashlib.sha256(f.read_bytes()).hexdigest()) for f in files]


async def migrate(conn: psycopg.AsyncConnection) -> list[str]:
    """Apply pending migrations; returns the versions applied by this call."""
    applied_now: list[str] = []
    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_ID,))
        await conn.execute(
            """CREATE TABLE IF NOT EXISTS schema_migrations (
                   version    text PRIMARY KEY,
                   checksum   text NOT NULL,
                   applied_at timestamptz NOT NULL DEFAULT now())"""
        )
        cur = await conn.execute("SELECT version, checksum FROM schema_migrations")
        applied = dict(await cur.fetchall())
        for version, sql, checksum in _migrations():
            if version in applied:
                if applied[version] != checksum:
                    raise RuntimeError(f"migration {version} was modified after being applied")
                continue
            await conn.execute(sql)
            await conn.execute(
                "INSERT INTO schema_migrations (version, checksum) VALUES (%s, %s)", (version, checksum)
            )
            applied_now.append(version)
    return applied_now


async def status(conn: psycopg.AsyncConnection) -> list[tuple[str, str]]:
    await conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())")
    cur = await conn.execute("SELECT version FROM schema_migrations")
    done = {row[0] for row in await cur.fetchall()}
    return [(v, "applied" if v in done else "pending") for v, _, _ in _migrations()]


async def _main(cmd: str) -> None:
    from app.config import get_settings

    url = os.environ.get("DATABASE_URL") or get_settings().database_url
    async with await psycopg.AsyncConnection.connect(url) as conn:
        if cmd == "up":
            done = await migrate(conn)
            print("applied: " + ", ".join(done) if done else "up to date")
        elif cmd == "status":
            for version, state in await status(conn):
                print(f"{version:40s} {state}")
        else:
            raise SystemExit(f"unknown command {cmd!r}")


if __name__ == "__main__":
    asyncio.run(_main(sys.argv[1] if len(sys.argv) > 1 else "up"))
