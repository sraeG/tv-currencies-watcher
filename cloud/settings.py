"""Shared settings & constants for the cloud scraper."""
from __future__ import annotations
import os
import random
from dataclasses import dataclass


DEFAULT_UA = (
"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
"AppleWebKit/537.36 (KHTML, like Gecko) "
"Chrome/120.0 Safari/537.36"
)


RECENT_CURRENCIES_URL = (
"https://www.tradingview.com/markets/currencies/ideas/?sort=recent"
)


# Jitter range in seconds to be polite on each run
JITTER_LOW = int(os.getenv("JITTER_LOW", "10"))
JITTER_HIGH = int(os.getenv("JITTER_HIGH", "20"))


# Network timeouts
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "15"))
READ_TIMEOUT = float(os.getenv("READ_TIMEOUT", "25"))


# Simple retry settings for transient network issues
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_SECS = int(os.getenv("RETRY_BACKOFF_SECS", "5"))


# Database URL must be provided by GitHub Actions secret
DATABASE_URL = os.getenv("DATABASE_URL", "")


USER_AGENT = os.getenv("USER_AGENT", DEFAULT_UA)


SOURCE_PAGE = "currencies_recent"


@dataclass
class RunStats:
new: int = 0
skipped: int = 0
