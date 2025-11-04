from __future__ import annotations
import json
from typing import Any, Dict, Iterable, List, Optional
from bs4 import BeautifulSoup

# ---------- Listing (your anchor logic) ----------

def parse_listing_for_uuids_and_links(html: str) -> List[Dict[str, str]]:
    """
    Extract /chart/<symbol>/<uuid> links from the listing HTML.
    Returns list of dicts: {"uuid": <uuid>, "url": <full idea URL>}
    """
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/chart/" not in href:
            continue
        # Normalize to absolute URL
        if href.startswith("/"):
            full = "https://www.tradingview.com" + href
        else:
            full = href
        parts = full.strip("/").split("/")
        # Expect .../chart/<symbol>/<uuid>...
        try:
            idx = parts.index("chart")
            symbol = parts[idx + 1]
            uuid = parts[idx + 2].split("-")[0]
            url = f"https://www.tradingview.com/chart/{symbol}/{uuid}"
            urls.append({"uuid": uuid, "url": url})
        except Exception:
            continue

    # de-dupe keep order
    seen = set()
    out = []
    for item in urls:
        if item["uuid"] in seen:
            continue
        seen.add(item["uuid"])
        out.append(item)
    return out

# ---------- Detail page parsing (your semantics) ----------

def _safe_get(d: dict, path: Iterable) -> Optional[Any]:
    cur: Any = d
    for key in path:
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        elif isinstance(cur, list) and isinstance(key, int) and 0 <= key < len(cur):
            cur = cur[key]
        else:
            return None
    return cur

def iso_to_epoch(iso_str: Optional[str]) -> Optional[int]:
    if not iso_str:
        return None
    try:
        from datetime import datetime
        # Accept trailing Z
        return int(datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None

def _extract_elements_from_content(content: Any) -> List[dict]:
    """
    ONLY objects whose type contains "LineTool" from either:
      ["content","panes",0,"sources"]
      ["content","charts",0,"panes",0,"sources"]
    For each element store {"type","state","points","indexes"} where present.
    """
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except Exception:
            return []

    paths = [
        ["panes", 0, "sources"],
        ["charts", 0, "panes", 0, "sources"],
    ]

    out: List[dict] = []
    for p in paths:
        sources = _safe_get(content, p)
        if isinstance(sources, list):
            for item in sources:
                if not isinstance(item, dict):
                    continue
                typ = item.get("type", "")
                if isinstance(typ, str) and "LineTool" in typ:
                    out.append({
                        "type": item.get("type"),
                        "state": item.get("state"),
                        "points": item.get("points"),
                        "indexes": item.get("indexes"),
                    })
    return out

def parse_detail_page(html: str) -> Dict[str, Any]:
    """
    Find <script type="application/prs.init-data+json">, DFS to ssrIdeaData,
    decode content if JSON (best-effort), build your exact field set + pricescale.
    """
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script", {"type": "application/prs.init-data+json"})

    def _deep_find(data: Any, key: str) -> Optional[dict]:
        if isinstance(data, dict):
            if key in data:
                return data[key]
            for v in data.values():
                res = _deep_find(v, key)
                if res is not None:
                    return res
        elif isinstance(data, list):
            for v in data:
                res = _deep_find(v, key)
                if res is not None:
                    return res
        return None

    idea = None
    for s in scripts:
        if not s.string:
            continue
        try:
            j = json.loads(s.string)
        except Exception:
            continue
        idea = _deep_find(j, "ssrIdeaData")
        if isinstance(idea, dict):
            break

    if not isinstance(idea, dict):
        # Return empty-but-typed structure (with pricescale=None)
        return {
            "username": None,
            "symbol": None,
            "created_at": None,
            "interval": None,
            "direction": None,
            "data": {
                "chart_url": None,
                "name": None,
                "webp_url": None,
                "updated_at": None,
                "likes_count": None,
                "comments_count": None,
                "views": None,
                "description_ast": None,
                "updates": None,
                "elements": [],
                "pricescale": None,
            },
        }

    # Decode content JSON only for elements extraction; keep rest as-is
    content = idea.get("content")
    elements = _extract_elements_from_content(content)

    sym_obj = idea.get("symbol") or {}
    # TradingView typically uses 'pricescale', but some payloads use 'price_scale'
    pricescale = sym_obj.get("pricescale")
    if pricescale is None:
        pricescale = sym_obj.get("price_scale")

    data_obj = {
        "chart_url": idea.get("publicPath") or idea.get("chart_url"),
        "name": idea.get("name"),
        "webp_url": idea.get("webpUrl") or idea.get("webp_url"),
        "updated_at": idea.get("updated_at"),
        "likes_count": idea.get("likes_count"),
        "comments_count": idea.get("comments_count"),
        "views": idea.get("views"),
        "description_ast": idea.get("description_ast"),
        "updates": idea.get("updates"),
        "elements": elements,
        "pricescale": pricescale,
    }

    return {
        "username": (idea.get("user") or {}).get("username"),
        "symbol": (sym_obj or {}).get("short_name") or "NONE",
        "created_at": iso_to_epoch(idea.get("created_at")),
        "interval": idea.get("interval"),
        "direction": idea.get("direction"),
        "data": data_obj,
    }
