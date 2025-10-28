from __future__ import annotations


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
# Can't parse expected JSON; return minimal
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


# content might be a JSON string; leave decoding for elements only
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
