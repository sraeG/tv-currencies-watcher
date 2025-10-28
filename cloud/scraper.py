from __future__ import annotations
import os
import random
import sys
import time
from dataclasses import asdict
from typing import List

import requests
from bs4 import BeautifulSoup  # noqa: F401 (import ensures bs4 present per requirements)
from pathlib import Path

from settings import (
    DATABASE_URL,
    JITTER_LOW,
    JITTER_HIGH,
    MAX_RETRIES,
    READ_TIMEOUT,
    CONNECT_TIMEOUT,
    RETRY_BACKOFF_SECS,
    USER_AGENT,
    RECENT_CURRENCIES_URL,
    SOURCE_PAGE,
    RunStats,
)
from db import make_engine, create_tables, insert_first_seen, has_uuid, upsert_full_record
from parsing import parse_listing_for_uuids_and_links, parse_detail_page

def _debug_save(name: str, content: str) -> None:
    if not DEBUG_HTML:
        return
    p = DEBUG_DIR / name
    try:
        p.write_text(content, encoding="utf-8", errors="ignore")
        print(f"[DEBUG] saved HTML -> {p}")
    except Exception as e:
        print(f"[DEBUG] failed to save {name}: {e}")

def _debug_print_html_stats(label: str, html: str) -> None:
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.string.strip() if soup.title and soup.title.string else "")
    scripts = soup.find_all("script", {"type": "application/prs.init-data+json"})
    print(
        f"[DEBUG] {label}: len={len(html)} title={title!r} prs.init-data+json tags={len(scripts)}"
    )
    # Show a tiny snippet of the first PRS JSON (without dumping everything)
    if scripts:
        snippet = scripts[0].string[:200].replace("\n", " ") if scripts[0].string else ""
        print(f"[DEBUG] {label}: first prs.init-data+json starts with: {snippet!r}")

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
    # Small polite jitter each run
    jitter = random.randint(JITTER_LOW, JITTER_HIGH)
    time.sleep(jitter)

    engine = make_engine(DATABASE_URL)
    create_tables(engine)

    stats = RunStats()

    with engine.begin() as conn:
        from sqlalchemy.orm import Session

        session = Session(bind=conn)

        # 1) Fetch listing
        listing_html = http_get(RECENT_CURRENCIES_URL)
        _debug_print_html_stats("listing", listing_html)
        _debug_save("listing.html", listing_html)
        items = parse_listing_for_uuids_and_links(listing_html)
        print(f"Listing items found: {len(items)}")


        # 2) Iterate newest -> older, stop at first known
        saved_detail = 0
        for item in items:
            uuid = item["uuid"]
            url = item["url"]
    
            if has_uuid(session, uuid):
                print(f"SKIP {uuid} (already seen)")
                stats.skipped += 1
                break  # Stop scanning listing on first known
    
            detail_html = http_get(url)
    
            # Save first few detail pages to artifacts for debugging
            if DEBUG_HTML and saved_detail < 3:
                _debug_print_html_stats(f"detail {uuid}", detail_html)
                _debug_save(f"idea_{uuid}.html", detail_html)
                saved_detail += 1
    
            parsed = parse_detail_page(detail_html)

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

            elements_count = len((parsed.get("data") or {}).get("elements", []) or [])
            print(
                f"NEW {uuid} {(parsed.get('symbol') or 'NONE')} "
                f"elements={elements_count}"
            )
            stats.new += 1

        session.commit()

    # Final line for CI logs
    print(f"DONE new={stats.new} skipped={stats.skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
