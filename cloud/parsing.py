from __future__ import annotations
import json
import re
from typing import Any, Dict, Iterable, List, Optional
from bs4 import BeautifulSoup

# ---- Utilities ----

def find_prs_init_json(html: str) -> Optional[dict]:
    """Return the decoded JSON from <script type="application/prs.init-data+json"> if present."""
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("script", {"type": "application/prs.init-data+json"})
    if not tag or not tag.string:
        return None
    try:
        return json.loads(tag.string)
    except Exception:
        return None


def deep_find_key(obj: Any, key: str) -> Optional[Any]:
    """DFS search for the first dict that has `key` as a key; returns that value."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = deep_find_key(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = deep_find_key(item, key)
            if found is not None:
                return found
    return None


# ---- Listing page ----

def _parse_listing_from_prs_json(html: str) -> List[Dict[str, str]]:
    """
    Prefer extracting from the listing page's prs.init-data+json.
    We look for an array of idea-like objects that contain uuid + publicPath (or url).
    """
    out: List[Dict[str, str]] = []
    seen = set()
    init_json = find_prs_init_json(html)
    if not isinstance(init_json, dict):
        return out

    # Common containers we've seen: 'ideas', 'items', 'list', nested 'ideas' under other keys
    candidates = []
    # Try a few likely keys first
    for k in ["ideas", "items", "list"]:
        v = init_json.get(k)
        if isinstance(v, list):
            candidates.append(v)

    # Generic deep search as a fallback
    if not candidates:
        v = deep_find_key(init_json, "ideas")
        if isinstance(v, list):
            candidates.append(v)

    # Flatten and extract uuid/publicPath
    for arr in candidates:
        for it in arr:
            if not isinstance(it, dict):
                continue
            uuid = it.get("uuid")
            url = it.get("publicPath") or it.get("url")
            # Some feeds store a relative URL; normalize
            if isinstance(url, str) and url.startswith("/"):
                url = "https://www.tradingview.com" + url
            if uuid and url and uuid not in seen:
                seen.add(uuid)
                out.append({"uuid": uuid, "url": url})

    return out


def _parse_listing_from_anchors(html: str) -> List[Dict[str, str]]:
    """
    Fallback: scrape anchors for /chart/... or /idea/... patterns.
    """
    soup = BeautifulSoup(html, "html.parser")
    hrefs = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/chart/") or "/idea/" in href:
            hrefs.append(href)

    results: List[Dict[str, str]] = []
    seen = set()
    for href in hrefs:
        url = href
        if url.startswith("/"):
            url = "https://www.tradingview.com" + url

        uuid = None
        m = re.search(r"/chart/([A-Za-z0-9]+)/", href)
        if m:
            uuid = m.group(1)
        else:
            m2 = re.search(r"/idea/[^/]+/([A-Za-z0-9]+)", href)
            if m2:
                uuid = m2.group(1)

        if uuid and uuid not in seen:
            seen.add(uuid)
            results.append({"uuid": uuid, "url": url})

    return results


def parse_listing_for_uuids_and_links(html: str) -> List[Dict[str, str]]:
    """
    Extract newest→older ideas from the currencies recent page.
    1) Try JSON script (prs.init-data+json) first.
    2) Fallback to anchor scanning.
    """
    items = _parse_listing_from_prs_json(html)
    if not items:
        items = _parse_listing_from_anchors(html)
    return items


# ---- Detail page ----

def iso_to_epoch(iso_str: Optional[str]) -> Optional[int]:
    if not iso_str:
        return None
    try:
        from dateutil import parser as dtparser
        dt = dtparser.isoparse(iso_str)
        return int(dt.timestamp())
    except Exception:
        return None


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


def extract_elements_from_content(content: Any) -> List[dict]:
    """
    Extract ONLY objects whose type contains "LineTool" from either
    ["content","panes",0,"sources"] or ["content","charts",0,"panes",0,"sources"].
    For each element store {"type","state","points","indexes"} when present.
    `content` can be a dict or a JSON string; handle both.
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
            for obj in sources:
                if not isinstance(obj, dict):
                    continue
                typ = obj.get("type")
                if isinstance(typ, str) and "LineTool" in typ:
                    entry = {
                        "type": typ,
                        "state": obj.get("state"),
                        "points": obj.get("points"),
                        "indexes": obj.get("indexes"),
                    }
                    out.append(entry)
    return out


def parse_detail_page(html: str) -> Dict[str, Any]:
    """
    Parse TV idea detail HTML to the expected fields.
    We search for `ssrIdeaData` within <script type="application/prs.init-data+json">,
    then map to our neutral dict.
    """
    init_json = find_prs_init_json(html)
    idea = None
    if init_json is not None:
        idea = deep_find_key(init_json, "ssrIdeaData")

    if not isinstance(idea, dict):
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
            },
        }

    content = idea.get("content")

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
        "elements": extract_elements_from_content(content),
    }

    return {
        "username": (idea.get("user") or {}).get("username"),
        "symbol": (idea.get("symbol") or {}).get("short_name") or "NONE",
        "created_at": iso_to_epoch(idea.get("created_at")),
        "interval": idea.get("interval"),
        "direction": idea.get("direction"),
        "data": data_obj,
    }
