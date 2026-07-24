import re

_OFFER_URL = "https://www.cian.ru/sale/flat/{}/"
_STATUS = {"new": 2, "resale": 1}


def api_headers(base):
    h = dict(base)
    h["Content-Type"] = "application/json"
    h["Accept"] = "*/*"
    h["Origin"] = "https://www.cian.ru"
    h["Referer"] = "https://www.cian.ru/"
    return h


def build_json_query(filt, cfg, page=1):
    region = cfg["regions"][filt["region"]]
    rooms = [int(n) for n in re.findall(r"room(\d+)=", region["rooms"][filt["room"]])]
    q = {
        "_type": "flatsale",
        "engine_version": {"type": "term", "value": 2},
        "region": {"type": "terms", "value": [region["id"]]},
        "room": {"type": "terms", "value": rooms},
        "page": {"type": "term", "value": page},
    }
    lo, hi = filt.get("price_lo"), filt.get("price_hi")
    if lo or hi:
        rng = {}
        if lo:
            rng["gte"] = lo
        if hi:
            rng["lte"] = hi
        q["price"] = {"type": "range", "value": rng}
    status = _STATUS.get(filt.get("otype"))
    if status:
        q["building_status"] = {"type": "term", "value": status}
    return {"jsonQuery": q}


def build_jk_query(jk_id, page=1):
    return {"jsonQuery": {
        "_type": "flatsale",
        "engine_version": {"type": "term", "value": 2},
        "geo": {"type": "geo", "value": [{"type": "newobject", "id": int(jk_id)}]},
        "page": {"type": "term", "value": page},
    }}


def parse_search(data):
    rows = []
    for o in data.get("offersSerialized") or []:
        cid = o.get("cianId") or o.get("id")
        if not cid:
            continue
        terms = o.get("bargainTerms") or {}
        price = terms.get("priceRur") or terms.get("price")
        jk_raw = (o.get("newbuilding") or {}).get("id")
        try:
            jk = int(jk_raw) if jk_raw is not None else None
        except (TypeError, ValueError):
            jk = None
        rows.append((_OFFER_URL.format(cid), int(price) if price else None, jk, o))
    return data.get("offerCount"), rows
