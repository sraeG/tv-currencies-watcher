from __future__ import annotations
import os
import sys
import json
import re
import requests
from bs4 import BeautifulSoup

USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36")
TIMEOUT = (15, 25)

def http_get(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def deep_find(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            f = deep_find(v, key)
            if f is not None:
                return f
    elif isinstance(obj, list):
        for v in obj:
            f = deep_find(v, key)
            if f is not None:
                return f
    return None

def main():
    if len(sys.argv) < 2:
        print("Usage: python cloud/dump_idea_json.py <idea-url | symbol uuid>")
        print("Examples:")
        print("  python cloud/dump_idea_json.py https://www.tradingview.com/chart/EURUSD/abcdef12/")
        print("  python cloud/dump_idea_json.py EURUSD abcdef12")
        sys.exit(1)

    if len(sys.argv) == 3:
        symbol, uuid = sys.argv[1], sys.argv[2]
        url = f"https://www.tradingview.com/chart/{symbol}/{uuid}"
    else:
        url = sys.argv[1]

    html = http_get(url)
    soup = BeautifulSoup(html, "html.parser")
    tags = soup.find_all("script", {"type": "application/prs.init-data+json"})
    print(f"[INFO] Found {len(tags)} prs.init-data+json scripts on page")

    idea = None
    for t in tags:
        if not t.string:
            continue
        try:
            j = json.loads(t.string)
        except Exception:
            continue
        idea = deep_find(j, "ssrIdeaData")
        if isinstance(idea, dict):
            break

    if not isinstance(idea, dict):
        print("[WARN] ssrIdeaData not found")
        sys.exit(2)

    # Print high-level keys
    print("[INFO] idea keys:", sorted(list(idea.keys())))

    # Show symbol object
    sym = idea.get("symbol") or {}
    print("[INFO] symbol keys:", sorted(list(sym.keys())))
    print("[INFO] symbol snippet:", json.dumps({k: sym.get(k) for k in ["short_name", "pricescale", "price_scale", "minmov", "minmove"]}, indent=2))

    # Try to locate pricescale anywhere in idea JSON (case-insensitive)
    idea_text = json.dumps(idea)
    if re.search(r'price[_\- ]?scale', idea_text, flags=re.I):
        print("[INFO] Found a 'price...scale' key somewhere in idea JSON")
    else:
        print("[INFO] No 'price...scale' key text found in idea JSON")

    # Inspect sources for MainSeries
    def safe_get(d, path):
        cur = d
        for p in path:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            elif isinstance(cur, list) and isinstance(p, int) and 0 <= p < len(cur):
                cur = cur[p]
            else:
                return None
        return cur

    sources_paths = [
        ["content", "panes", 0, "sources"],
        ["content", "charts", 0, "panes", 0, "sources"],
    ]
    for p in sources_paths:
        sources = safe_get(idea, p) or []
        print(f"[INFO] path {p} -> {len(sources) if isinstance(sources, list) else 0} sources")
        if isinstance(sources, list):
            for s in sources[:5]:
                t = s.get("type")
                st = s.get("state") or {}
                print("   - type:", t, "| state.keys:", list(st.keys())[:10], "| pricescale in state:", st.get("pricescale"))

    # Pretty-print a compact view of ssrIdeaData for manual inspection
    print("\n[INFO] Compact ssrIdeaData dump (truncated big fields):")
    dump = dict(idea)
    # strip huge fields for readability
    '''for k in ["content", "description_ast", "updates"]:
        if k in dump:
            dump[k] = f"<{k} omitted>"'''
    print(json.dumps(dump, indent=2)[:4000])  # truncate to keep logs sane

if __name__ == "__main__":
    main()
