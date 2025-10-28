from __future__ import annotations
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




def http_get(url: str) -> str:
headers = {"User-Agent": USER_AGENT}
last_err = None
for attempt in range(1, MAX_RETRIES + 1):
try:
resp = requests.get(
url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
)
if 200 <= resp.status_code < 300:
return resp.text
else:
last_err = RuntimeError(f"HTTP {resp.status_code} for {url}")
except Exception as e: # network timeouts etc.
last_err = e
if attempt < MAX_RETRIES:
time.sleep(RETRY_BACKOFF_SECS * attempt)
raise last_err # type: ignore[misc]




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
items = parse_listing_for_uuids_and_links(listing_html)


# 2) Iterate newest -> older, stop at first known
for item in items:
uuid = item["uuid"]
url = item["url"]


if has_uuid(session, uuid):
print(f"SKIP {uuid} (already seen)")
stats.skipped += 1
break # Stop scanning listing on first known


# Record first-seen snapshot row
insert_first_seen(session, uuid, SOURCE_PAGE)


# Fetch detail only for new ideas
detail_html = http_get(url)
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
