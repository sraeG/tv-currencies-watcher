from __future__ import annotations
import os
import random
import sys
import time
from typing import List

import requests
from bs4 import BeautifulSoup  # ensure present

from settings import (
    DATABASE_URL,
    JITTER_LOW,
    JITTER_HIGH,
    MAX_RETRIES,
    READ_TIMEOUT,
    CONNECT_TIMEOUT,
    RETRY_BACKOFF_SECS,
    USER_AGENT,
    RECENT_LISTING_URL,
    SOURCE_PAGE,
    RunStats,
)
from db import make_engine, create_tables, insert_first_seen, has_uuid, upsert_full_record
from parsing import parse_listing_for_uuids_and_links, parse_detail_page


def http_get(url: str) -> str:
    headers = {"User-Agent": USER_AGENT}
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            print(f"[HTTP] GET {url} -> {resp.status_code}")
            if 200 <= resp.status_code < 300:
                return resp.text
            else:
                last_err = RuntimeError(f"HTTP {resp.status_code} for {url}")
        except Exception as e:
            last_err = e
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF_SECS * attempt)
    raise last_err  # type: ignore[misc]


def main() -> int:
    # Polite jitter per run
    jitter = random.randint(JITTER_LOW, JITTER_HIGH)
    time.sleep(jitter)

    # DB init
    engine = make_engine(DATABASE_URL)
    create_tables(engine)

    stats = RunStats()
    from sqlalchemy.orm import Session

    with engine.begin() as conn:
        session = Session(bind=conn)

        # 1) Fetch listing
        print(f"[DEBUG] Requesting listing page: {RECENT_LISTING_URL}")
        listing_html = http_get(RECENT_LISTING_URL)
        print(f"[DEBUG] Listing page fetched, length={len(listing_html)}")

        items = parse_listing_for_uuids_and_links(listing_html)
        print(f"[DEBUG] Parsed {len(items)} idea items from listing")
        if items:
            print("[DEBUG] First 5 idea URLs:", [it["url"] for it in items[:5]])

        # 2) Iterate ALL items on the page; only fetch details for new UUIDs.
        #    (No more stop-at-first-known; avoids missing ideas due to sponsored/pinned posts.)
        for item in items:
            uuid = item["uuid"]
            url = item["url"]

            if has_uuid(session, uuid):
                print(f"SKIP {uuid} (already seen)")
                stats.skipped += 1
                continue  # <-- key change: do NOT break; just skip this one

            print(f"[DEBUG] Visiting idea {uuid} at {url}")
            detail_html = http_get(url)
            parsed = parse_detail_page(detail_html)

            sym = (parsed.get("symbol") or "NONE") or "NONE"
            # Forex 6-letter pairs (letters) OR XAU* (e.g., XAUUSD, XAUEUR, etc.)
            is_fx = (isinstance(sym, str) and ((len(sym) == 6 and sym.isalpha()) or sym.upper().startswith("XAU")))
            if not is_fx:
                print(f"[DEBUG] Non-FX/XAU idea {uuid} sym={sym}; ignoring")
                # Do NOT insert first_seen for out-of-scope
                continue

            # Record first-seen only for in-scope ideas
            insert_first_seen(session, uuid, SOURCE_PAGE)

            upsert_full_record(
                session,
                uuid=uuid,
                username=parsed.get("username"),
                symbol=sym,
                created_at=parsed.get("created_at"),
                interval=parsed.get("interval"),
                direction=parsed.get("direction"),
                data=parsed.get("data"),
            )

            elements_count = len((parsed.get("data") or {}).get("elements", []) or [])
            print(f"NEW {uuid} {sym} elements={elements_count}")
            stats.new += 1

        session.commit()

    print("[DEBUG] Finished run, summary:")
    print(f"DONE new={stats.new} skipped={stats.skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
