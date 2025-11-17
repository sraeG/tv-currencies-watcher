# cloud/scraper.py
from __future__ import annotations
import random
import sys
import time

import requests
from sqlalchemy.orm import Session

from settings import (
    DATABASE_URL,
    JITTER_LOW,
    JITTER_HIGH,
    MAX_RETRIES,
    READ_TIMEOUT,
    CONNECT_TIMEOUT,
    RETRY_BACKOFF_SECS,
    USER_AGENT,
    RECENT_LISTING_URL,  # now pointing to ALL IDEAS by default
    SOURCE_PAGE,         # e.g. 'ideas_recent'
    RunStats,
)
from db import make_engine, create_tables, insert_first_seen, has_uuid, upsert_full_record
from parsing import (
    parse_listing_for_uuids_and_links,
    parse_detail_page,
)


def http_get(url: str) -> str:
    """HTTP GET with retries/backoff; raises if exhausted."""
    headers = {"User-Agent": USER_AGENT}
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            print(f"[HTTP] GET {url} -> {resp.status_code}")
            if 200 <= resp.status_code < 300:
                return resp.text
            last_err = RuntimeError(f"HTTP {resp.status_code} for {url}")
        except Exception as e:
            last_err = e
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF_SECS * attempt)
    # Exhausted retries
    raise last_err if last_err else RuntimeError(f"GET failed for {url}")


def main() -> int:
    # Polite per-run jitter
    jitter = random.randint(JITTER_LOW, JITTER_HIGH)
    time.sleep(jitter)

    # DB init
    engine = make_engine(DATABASE_URL)
    create_tables(engine)

    stats = RunStats()

    with engine.begin() as conn:
        session = Session(bind=conn)

        # 1) Fetch the ALL-IDEAS listing page
        print(f"[DEBUG] Requesting listing page: {RECENT_LISTING_URL}")
        listing_html = http_get(RECENT_LISTING_URL)
        print(f"[DEBUG] Listing page fetched, length={len(listing_html)}")

        items = parse_listing_for_uuids_and_links(listing_html)
        print(f"[DEBUG] Parsed {len(items)} idea items from listing")
        if items:
            print("[DEBUG] First 5 idea URLs:", [it["url"] for it in items[:5]])

        # 2) Iterate ALL items; only fetch details for brand-new UUIDs
        for item in items:
            uuid = item["uuid"]
            url = item["url"]

            if has_uuid(session, uuid):
                print(f"SKIP {uuid} (already seen)")
                stats.skipped += 1
                continue

            # Detail page (build full record)
            print(f"[DEBUG] Visiting idea {uuid} at {url}")
            detail_html = http_get(url)
            parsed = parse_detail_page(detail_html)

            # Record first-seen + upsert
            insert_first_seen(session, uuid, SOURCE_PAGE)
            upsert_full_record(
                session,
                uuid=uuid,
                username=parsed.get("username"),
                symbol=parsed.get("symbol"),
                created_at=parsed.get("created_at"),
                interval=parsed.get("interval"),
                direction=parsed.get("direction"),
                data=parsed.get("data"),
            )

            data = parsed.get("data") or {}
            elements_count = len(data.get("elements", []) or [])
            ps = data.get("pricescale")
            print(
                f"NEW {uuid} {parsed.get('symbol')} "
                f"elements={elements_count} pricescale={ps}"
            )
            stats.new += 1

        session.commit()

    print("[DEBUG] Finished run, summary:")
    print(f"DONE new={stats.new} skipped={stats.skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
