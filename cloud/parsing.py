from __future__ import annotations
import json
import re
from typing import Any, Dict, Iterable, List, Optional
from bs4 import BeautifulSoup

# ---- Utilities ----

def find_all_prs_init_jsons(html: str) -> List[dict]:
    """
    Return decoded JSON objects from ALL <script type="application/prs.init-data+json"> tags.
    Ignore any that fail to decode.
    """
    soup = BeautifulSoup(html, "html.parser")
    out: List[dict] = []
    for tag in soup.find_all("script", {"type": "application/prs.init-data+json"}):
        if not tag.string:
            continue
        try:
            data = json.loads(tag.string)
            if isinstance(data, (dict, list)):
                out.append(data)
        except Exception:
            continue
    return out


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

def _collect_idea_like_dicts(obj: Any, out: List[Dict[str, str]]) -> None:
    """
    Walk arbitrary JSON and collect dicts that look like idea cards, i.e.,
    have a UUID and a URL/publicPath to the idea page.
    """
    if isinstance(obj, dict):
        # Heuristics:
        # - Many feeds use 'publicPath' like '/idea/<slug>/<uuid>/'
        # - Some have 'url' fields that are relative and include '/idea/'
        # - Ensure we have a 'uuid' alongside.
        uuid = obj.get("uuid")
        public_path = obj.get("publicPath") or obj.get("public_path")
        url = obj.get("url") or obj.get("href")
        cand_url = None

        if isinstance(public_path, str) and "/idea/" in public_path:
            cand_url = public_path
        elif isinstance(url, str) and "/idea/" in url:
            cand_url = url

        if uuid and isinstance(uuid, str) and cand_url:
            if cand_url.startswith("/"):
                cand_url = "https://www.tradingview.com" + cand_url
            out.append({"uuid": uuid, "url": cand_url})

        # Recurse
        for v in obj.values():
            _collect_idea_like_dicts(v, out)

    elif isinstance(obj, list):
        for item in obj:
            _collect_idea_like_dicts(item, out)


def _parse_listing_from_prs_jsons(html: str) -> List[Dict[str, str]]:
    all_jsons = find_all_prs_init_jsons(html)
    candidates: List[Dict[str, str]] = []
    for j in all_jsons:
        _collect_idea_like_dicts(j, candidates)

    # De-dupe by uuid, keep order
    seen = set()
    out: List[Dict[str, str]] = []
    for it in candidates:
        u = it.get("uuid")
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(it)
    return out


def _parse_listing_from_anchors(html: str) -> List[Dict[str, str]]:
    """
    Fallback: scrape anchors for /idea/ patterns (more restrictive than before).
    """
    soup = BeautifulSoup(html, "html.parser")
    results: List[Dict[str, str]] = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/idea/" not in href:
            continue
        url = href
        if url.startswith("/"):
            url = "https://www.tradingview.com" + url

        # Try to extract a plausible UUID token at the end of /idea/<slug>/<uuid>[/]
        m = re.search(r"/idea/[^/]+/([A-Za-z0-9]+)(?:/|$)", url)
        if not m:
            continue
        uuid = m.group(1)
        if uuid in seen:
            continue
        seen.add(uuid)
        results.append({"uuid": uuid, "url": url})
    return results


def parse_listing_for_uuids_and_links(html: str) -> List[Dict[str, str]]:
    """
    Extract newest→older ideas from the currencies recent page.
    1) Try aggregating over ALL prs.init-data+json scripts.
    2) Fallback to anchor scanning for /idea/ links.
    """
    items = _parse_listing_from_prs_jsons(html)
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
    We search for `ssrIdeaData` within ANY of the prs.init-data+json scripts,
    then map to our neutral dict.
    """
    # Use the multi-script reader here too (some idea pages also have >1 tag)
    soup = BeautifulSoup(html, "html.parser")
    idea = None
    for tag in soup.find_all("script", {"type": "application/prs.init-data+json"}):
        if not tag.string:
            continue
        try:
            data = json.loads(tag.string)
        except Exception:
            continue
        idea = deep_find_key(data, "ssrIdeaData")
        if isinstance(idea, dict):
            break

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
