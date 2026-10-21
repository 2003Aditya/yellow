import os
import socket
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    instance_id: str = f"{socket.gethostname()}:{os.getpid()}"

    database_url: str = "postgresql://coupon:coupon@localhost:55432/coupons"
    db_pool_min: int = 2
    db_pool_max: int = 20
    db_pool_timeout_s: float = 5.0  # waiting for a free connection
    db_connect_timeout_s: float = 30.0  # waiting for postgres on startup
    statement_timeout_ms: int = 3000
    lock_timeout_ms: int = 2000
    deadlock_retries: int = 3

    # Bulkhead: max concurrent redeem transactions per coupon per instance. A hot flash-sale
    # coupon can then never occupy the whole pool and starve requests for other coupons.
    redeem_max_inflight_per_coupon: int = 8
    redeem_queue_timeout_s: float = 3.0

    # Rate limits: requests per window, per endpoint. Counters live in Postgres (shared by all
    # instances) and use their own small pool so checkout traffic can't starve the limiter.
    rate_limit_enabled: bool = True
    rate_limit_window_s: int = 10
    rate_limit_pool_size: int = 4
    rate_limit_timeout_s: float = 0.2  # limiter DB budget; on failure it fails open
    rate_limit_redeem_per_customer: int = 10
    rate_limit_redeem_per_ip: int = 100
    rate_limit_coupon_create_per_ip: int = 20
    rate_limit_coupon_read_per_ip: int = 300
    rate_limit_cancel_per_ip: int = 60
    rate_limit_health_per_ip: int = 600

    cache_ttl_s: float = 30.0
    cache_max_entries: int = 10_000


@lru_cache
def get_settings() -> Settings:
    return Settings()
