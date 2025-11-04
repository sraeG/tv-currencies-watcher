from __future__ import annotations
import os
import sys
import time
from typing import Optional

import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from settings import (
    DATABASE_URL,
    USER_AGENT,
    CONNECT_TIMEOUT,
    READ_TIMEOUT,
    MAX_RETRIES,
    RETRY_BACKOFF_SECS,
)
from db import make_engine

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
ONLY_DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

from parsing import parse_detail_page  # uses the updated parser with pricescale

def http_get(url: str) -> str:
    headers = {"User-Agent": USER_AGENT}
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if 200 <= resp.status_code < 300:
                return resp.text
            last_err = RuntimeError(f"HTTP {resp.status_code} for {url}")
        except Exception as e:
            last_err = e
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF_SECS * attempt)
    raise last_err  # type: ignore[misc]

def pick_detail_url(chart_url: Optional[str], symbol: Optional[str], uuid: str) -> str:
    if chart_url and isinstance(chart_url, str):
        # chart_url might be relative
        if chart_url.startswith("/"):
            return "https://www.tradingview.com" + chart_url
        if chart_url.startswith("http"):
            return chart_url
    # fallback to standard chart URL
    sym = (symbol or "NONE")
    return f"https://www.tradingview.com/chart/{sym}/{uuid}"

def main() -> int:
    engine = make_engine(DATABASE_URL)

    with engine.begin() as conn:
        session = Session(bind=conn)

        # Fetch a batch of rows missing pricescale
        rows = session.execute(text("""
            select
              uuid,
              symbol,
              (data->>'chart_url') as chart_url
            from charts
            where (data->>'pricescale') is null
            order by first_seen_at asc
            limit :lim
        """), {"lim": BATCH_SIZE}).fetchall()

        if not rows:
            print("Nothing to backfill. All rows have pricescale.")
            return 0

        print(f"Backfilling up to {len(rows)} rows...")
        updated = 0
        skipped = 0

        for (uuid, symbol, chart_url) in rows:
            url = pick_detail_url(chart_url, symbol, uuid)
            try:
                html = http_get(url)
                parsed = parse_detail_page(html)
                ps = (parsed.get("data") or {}).get("pricescale")
                if ps is None:
                    print(f"MISS {uuid} (no pricescale found)")
                    skipped += 1
                    continue

                if ONLY_DRY_RUN:
                    print(f"DRY-RUN would set pricescale={ps} for {uuid}")
                    updated += 1
                    continue

                # Update data->'pricescale' and scraped_at with a JSONB set
                # jsonb_set(data, '{pricescale}', to_jsonb(:ps), true)
                now_epoch = int(time.time())
                session.execute(text("""
                    update charts
                    set data = jsonb_set(data, '{pricescale}', to_jsonb(:ps), true),
                        scraped_at = :now_epoch
                    where uuid = :uuid
                """), {"ps": ps, "now_epoch": now_epoch, "uuid": uuid})
                print(f"SET  {uuid} pricescale={ps}")
                updated += 1
            except Exception as e:
                print(f"ERR  {uuid} fetch/parse failed: {e}")
                skipped += 1

        print(f"DONE backfill updated={updated} skipped={skipped}")
        return 0

if __name__ == "__main__":
    sys.exit(main())
