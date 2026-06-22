import json
import re


def extract_cian_id(url: str) -> int | None:
    match = re.search(r"/flat/(\d+)", url)
    return int(match.group(1)) if match else None


def extract_region_id(html: str) -> int | None:
    match = re.search(r'"regionId":\s*(\d+)', html)
    return int(match.group(1)) if match else None


def _parse_float(text: str) -> float | None:
    match = re.search(r"[\d,\.]+", text)
    return float(match.group().replace(",", ".")) if match else None


def extract_offer_data(html: str) -> dict | None:
    marker = '"offerData":'
    i = html.find(marker)
    if i < 0:
        return None
    start = i + len(marker)
    depth = 0
    began = False
    for k in range(start, len(html)):
        c = html[k]
        if c == "{":
            depth += 1
            began = True
        elif c == "}":
            depth -= 1
            if began and depth == 0:
                try:
                    return json.loads(html[start:k + 1])
                except json.JSONDecodeError:
                    return None
    return None


_MICRO_TYPES = {"mikroraion", "microdistrict", "newobject", "settlement", "poselenie"}
_JK_RE = re.compile(r"жилой комплекс|жилой кв|квартал|микрорайон|мкр", re.IGNORECASE)


def normalize_district(name: str | None) -> str | None:
    if not name:
        return name
    name = re.sub(r"^(р-н|район)\s+", "", name)
    name = re.sub(r"\s+район$", "", name)
    return name.strip()


def _parse_geo_address(items: list) -> dict:
    res = {"region": None, "municipality": None, "district": None,
           "microdistrict": None, "street": None, "house": None}
    loc_seen = 0
    for it in items or []:
        name = it.get("shortName") or it.get("fullName") or it.get("name")
        if not name:
            continue
        t = it.get("type")
        if t == "location":
            loc_seen += 1
            if loc_seen == 1:
                res["region"] = it.get("fullName") or name
            elif _JK_RE.search(name):
                if res["microdistrict"] is None:
                    res["microdistrict"] = name
            elif res["municipality"] is None:
                res["municipality"] = name
            elif res["microdistrict"] is None:
                res["microdistrict"] = name
        elif t == "okrug":
            res["municipality"] = name
        elif t == "raion":
            res["district"] = normalize_district(name)
        elif t == "street":
            res["street"] = name
        elif t == "house":
            res["house"] = name
        elif t in _MICRO_TYPES and res["microdistrict"] is None:
            res["microdistrict"] = name
    return res


def _parse_metro_json(geo: dict) -> list | None:
    out = []
    for u in geo.get("undergrounds") or []:
        name = u.get("name")
        if name:
            out.append([name, u.get("travelTime"), u.get("travelType")])
    return out or None


def _parse_views(stats: dict | None) -> tuple[int | None, int | None]:
    if not stats:
        return None, None
    s = stats.get("totalViewsFormattedString") or ""
    m_total = re.match(r"\s*([\d\s\xa0]+)", s)
    total = int(re.sub(r"\D", "", m_total.group(1))) if m_total else None
    m_today = re.search(r",\s*([\d\s\xa0]+)\s+за сегодня", s)
    today = int(re.sub(r"\D", "", m_today.group(1))) if m_today else None
    return total, today


def _extract_completion_date(o: dict) -> str | None:
    quarter_names = {"first": 1, "second": 2, "third": 3, "fourth": 4}

    dl = (o.get("building") or {}).get("deadline") or {}
    if dl.get("year"):
        q = dl.get("quarter", "")
        qn = quarter_names.get(q, q) if isinstance(q, str) else q
        return f"{qn} кв. {dl['year']}" if qn else str(dl["year"])

    fd = ((o.get("newbuilding") or {}).get("house") or {}).get("finishDate") or {}
    if fd.get("year"):
        q = fd.get("quarter")
        return f"{q} кв. {fd['year']}" if q else str(fd["year"])

    info = ((o.get("newbuilding") or {}).get("newbuildingFeatures") or {}).get("deadlineInfo")
    if info:
        return info

    return None


def map_offer(od: dict) -> dict:
    o = od.get("offer") or {}
    geo = o.get("geo") or {}
    coords = geo.get("coordinates") or {}
    building = o.get("building") or {}
    terms = o.get("bargainTerms") or {}
    nb = o.get("newbuilding") or {}
    jk = geo.get("jk") or {}
    agent = od.get("agent") or {}

    addr = _parse_geo_address(geo.get("address") or [])

    total_area = o.get("totalArea")
    if isinstance(total_area, str):
        total_area = _parse_float(total_area)
    price = terms.get("price")
    price_per_m2 = int(price / total_area) if price and total_area else None

    is_studio = bool(o.get("isStudio")) or o.get("flatType") == "studio"

    wc_combined = o.get("combinedWcsCount") or 0
    wc_separate = o.get("separateWcsCount") or 0
    bathrooms = None
    if wc_combined or wc_separate:
        parts = []
        if wc_separate:
            parts.append(f"{wc_separate} разд.")
        if wc_combined:
            parts.append(f"{wc_combined} совм.")
        bathrooms = ", ".join(parts)

    balc = o.get("balconiesCount") or 0
    logg = o.get("loggiasCount") or 0
    balcony = None
    if balc or logg:
        parts = []
        if balc:
            parts.append(f"{balc} балк.")
        if logg:
            parts.append(f"{logg} лодж.")
        balcony = ", ".join(parts)

    views_total, views_today = _parse_views(od.get("stats"))
    parking = building.get("parking")
    parking_type = parking.get("type") if isinstance(parking, dict) and parking else None

    return {
        "price": price,
        "price_per_m2": price_per_m2,
        "price_type": terms.get("priceType"),
        "mortgage_allowed": terms.get("mortgageAllowed"),
        "deal_conditions": terms.get("saleType"),
        **addr,
        "lat": coords.get("lat"),
        "lon": coords.get("lng"),
        "metro_stations": _parse_metro_json(geo),
        "rooms": o.get("roomsCount"),
        "is_studio": is_studio,
        "flat_type": o.get("flatType"),
        "total_area": total_area,
        "living_area": o.get("livingArea"),
        "kitchen_area": o.get("kitchenArea"),
        "floor": o.get("floorNumber"),
        "total_floors": building.get("floorsCount"),
        "ceiling_height": building.get("ceilingHeight"),
        "renovation": o.get("repairType") or o.get("decoration"),
        "bathrooms": bathrooms,
        "balcony": balcony,
        "window_view": o.get("windowsViewType"),
        "is_apartments": o.get("isApartments"),
        "year_built": building.get("buildYear"),
        "building_type": building.get("materialType"),
        "parking": parking_type,
        "passenger_lifts": building.get("passengerLiftsCount") or o.get("passengerLiftsCount"),
        "cargo_lifts": building.get("cargoLiftsCount") or o.get("cargoLiftsCount"),
        "is_new_building": bool(nb.get("id") or nb.get("isFromDeveloper") or nb.get("isFromBuilder") or jk.get("name")),
        "developer": nb.get("name") or None,
        "residential_complex": jk.get("name") or None,
        "completion_date": _extract_completion_date(o),
        "description": o.get("description"),
        "publication_date": o.get("creationDate"),
        "edit_date": o.get("editDate"),
        "seller_type": agent.get("accountType"),
        "seller_user_type": agent.get("userType"),
        "phone_protected": not bool(o.get("phones")),
        "photos_count": len(o["photos"]) if o.get("photos") else None,
        "views_total": views_total,
        "views_today": views_today,
        "seller_is_owner": o.get("isByHomeowner"),
        "status": o.get("status"),
        "cian_user_id": o.get("cianUserId"),
        "is_penthouse": o.get("isPenthouse"),
        "room_type": o.get("roomType"),
        "demolished_in_renovation": o.get("demolishedInMoscowProgramm"),
    }


def parse_offer_from_json(html: str) -> tuple[dict | None, list[dict]]:
    od = extract_offer_data(html)
    if not od or "offer" not in od:
        return None, []

    data = map_offer(od)
    if not data.get("price"):
        return None, []

    price_history = [
        {"price": e["priceData"]["price"], "date": e["changeTime"][:10]}
        for e in (od.get("priceChanges") or [])
        if isinstance(e, dict) and e.get("priceData") and e.get("changeTime")
    ]
    return data, price_history


def parse_similar_urls_from_html(html: str) -> list[str]:
    raw = re.findall(r'href="(https?://[^"]*?/sale/flat/\d+/)"', html)
    seen = set()
    result = []
    for u in raw:
        clean = u.split("?")[0]
        if clean not in seen:
            seen.add(clean)
            result.append(clean)
    return result
