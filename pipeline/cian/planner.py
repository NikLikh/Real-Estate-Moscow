import asyncio
import logging
import math
import random
import re

from config.settings import load_scraper_config
from pipeline.cian.parsers import extract_cian_id
from pipeline.cian.runtime import load_checkpoint, save_checkpoint
from pipeline.core.raw_repo import fetch_price_bands, fetch_region_names

log = logging.getLogger("re")

_ISNEW = {"new": True, "resale": False}
_SPECIAL_REGIONS = {
    "Москва": "msk",
    "Московская область": "mo",
    "Санкт-Петербург": "spb",
    "Ленинградская область": "lo",
}
_STRIP_WORDS = ["автономная область", "автономный округ", "республика", "область", "край", "респ."]


def _norm_region(name):
    s = _SPECIAL_REGIONS.get(name, name).lower()
    for word in _STRIP_WORDS:
        s = s.replace(word, " ")
    return re.sub(r"[^a-zа-яё0-9]+", "_", s).strip("_")


def _room_keys(spec):
    codes = [int(n) for n in re.findall(r"room(\d+)=", spec)]
    if 9 in codes:
        return ["studio"]
    return sorted({"5+" if c >= 5 else str(c) for c in codes})


def _deals(cfg):
    return cfg.get("deals") or {"sale": {}}


def _deal_cfg(cfg, filt):
    return _deals(cfg).get(filt.get("deal") or "sale") or {}


def _build(cfg, bands_of):
    filters = []

    for deal in _deals(cfg):
        for region_name, region in cfg["regions"].items():
            otypes = region.get("object_types") or {None: None}
            if deal != "sale":
                otypes = {None: None}

            for room_name in region["rooms"]:
                for lo, hi in bands_of(region):
                    for otype_name in otypes:
                        label_parts = [deal, region_name, room_name, _price_label(lo, hi)]
                        if otype_name:
                            label_parts.append(otype_name)

                        filters.append(
                            {
                                "label": "/".join(label_parts),
                                "deal": deal,
                                "region": region_name,
                                "room": room_name,
                                "price_lo": lo,
                                "price_hi": hi,
                                "otype": otype_name,
                            }
                        )

    filters.sort(key=_priority_key)
    return filters


def build_filters_from_config(cfg=None):
    cfg = cfg or load_scraper_config()
    return _build(cfg, lambda region: region["prices"])


def build_root_filters(cfg=None):
    cfg = cfg or load_scraper_config()
    return _build(cfg, lambda region: [(None, None)])


def _priority_key(f):
    return random.random()


def _price_label(lo, hi):
    def fmt(v):
        if v is None:
            return ""
        if v >= 1_000_000:
            return f"{v / 1_000_000:g}M"
        return f"{v // 1000}K"

    return f"{fmt(lo)}-{fmt(hi) or '+'}"


def _derive_filter(parent, new_lo, new_hi):
    label_parts = [parent["deal"], parent["region"], parent["room"], _price_label(new_lo, new_hi)]
    if parent["otype"]:
        label_parts.append(parent["otype"])

    return {
        "label": "/".join(label_parts),
        "deal": parent["deal"],
        "region": parent["region"],
        "room": parent["room"],
        "price_lo": new_lo,
        "price_hi": new_hi,
        "otype": parent["otype"],
    }


async def _http_check_count(pool, filt, cfg, sem, sessions, max_retries=10):
    from curl_cffi.requests import AsyncSession
    from pipeline.cian.proxy_farm.detector import MAX_BODY_BYTES, headers as _headers, is_waf as _is_waf, is_captcha as _is_captcha
    from pipeline.cian.api import api_headers, build_json_query, parse_search

    body = build_json_query(filt, cfg)
    h = api_headers(_headers())

    for attempt in range(max_retries):
        slot = await pool.acquire()
        if not slot:
            await asyncio.sleep(3)
            continue

        if slot.proxy not in sessions:
            sessions[slot.proxy] = AsyncSession(
                impersonate="chrome", proxy=slot.proxy, max_clients=5
            )
        s = sessions[slot.proxy]

        async with sem:
            try:
                resp = await s.post(cfg["api_listing_endpoint"], json=body, headers=h, timeout=15)
            except Exception:
                pool.report_net_error(slot, cfg.get("net_error_threshold", 3),
                                      cfg.get("net_error_cooldown", 20),
                                      cfg.get("net_error_quarantine", 8))
                continue

            if len(resp.content) > MAX_BODY_BYTES:
                pool.report_waf(slot, 30)
                continue

            if _is_waf(resp.text, resp.status_code):
                pool.report_waf(slot, cfg.get("http_waf_cooldown", 35))
                continue

            if _is_captcha(resp.text, str(resp.url)):
                pool.report_captcha(slot, cfg.get("http_captcha_cooldown", 2),
                                    cfg.get("captcha_streak_limit", 6),
                                    cfg.get("captcha_streak_cooldown", 1800))
                continue

            pool.report_ok(slot)
            try:
                data = resp.json()["data"]
            except Exception:
                return None, []
            count, rows = parse_search(data)
            return count, [u for u, *_ in rows]

    return None, []


async def _http_maybe_split(pool, filt, max_offers, min_split, cfg, sem, sessions, early_urls=None):
    count, page1_urls = await _http_check_count(pool, filt, cfg, sem, sessions)
    if early_urls is not None and page1_urls:
        early_urls.extend(page1_urls)
    lo = filt["price_lo"] or 0
    hi = filt["price_hi"]

    if count is None:
        filt["offer_count"] = cfg.get("max_pages", 54) * 28
        filt["count_unknown"] = True
        log.info(f"  {filt['label']}: count unknown, ставим {filt['offer_count']} (max pages)")
        return [filt]

    filt["offer_count"] = count

    if count <= max_offers:
        log.info(f"  {filt['label']}: {count} offers, ok")
        return [filt]

    if hi is None:
        hi = lo * 3 if lo else _deal_cfg(cfg, filt).get("price_cap", 100_000_000)
    mid = (lo + hi) // 2

    if mid - lo < min_split or hi - mid < min_split:
        log.info(f"  {filt['label']}: {count} offers, дальше не дробим")
        return [filt]

    log.info(f"  {filt['label']}: {count} offers, дробим на {mid // 1_000_000}M")

    left = _derive_filter(filt, lo, mid - 1)
    right = _derive_filter(filt, mid, filt["price_hi"])

    left_res, right_res = await asyncio.gather(
        _http_maybe_split(pool, left, max_offers, min_split, cfg, sem, sessions),
        _http_maybe_split(pool, right, max_offers, min_split, cfg, sem, sessions),
    )
    return left_res + right_res


async def _plan_from_db(pool, cfg, sem, sessions, early_urls):
    roots = build_root_filters(cfg)
    target = cfg.get("plan_target_bucket", 150)
    max_offers = cfg.get("max_offers_per_filter", 200)
    min_split = cfg.get("min_price_split", 25_000)

    log.info(f"plan: {len(roots)} корневых сегментов, спрашиваем count у циана")
    counted = await asyncio.gather(*[
        _http_check_count(pool, filt, cfg, sem, sessions) for filt in roots
    ])

    keys = {_norm_region(k): k for k in cfg["regions"]}
    names = {}
    for name in fetch_region_names():
        key = keys.get(_norm_region(name))
        if key:
            names[key] = name

    result = []
    wanted = []
    index = {}
    fallback = []

    for filt, (count, urls) in zip(roots, counted):
        if early_urls is not None and urls:
            early_urls.extend(urls)
        if count is None:
            fallback.append(filt)
            continue
        filt["offer_count"] = count
        if count <= _deal_cfg(cfg, filt).get("max_offers", max_offers):
            result.append(filt)
            continue
        region = names.get(filt["region"])
        if not region or filt["deal"] != "sale":
            fallback.append(filt)
            continue
        index[filt["label"]] = filt
        wanted.append({
            "region": region,
            "rkey": filt["label"],
            "rks": _room_keys(cfg["regions"][filt["region"]]["rooms"][filt["room"]]),
            "isnew": _ISNEW.get(filt["otype"]),
            "k": math.ceil(count / target),
        })

    bands = fetch_price_bands(wanted)
    split_ok = 0

    for item in wanted:
        filt = index[item["rkey"]]
        band = bands.get(item["rkey"])
        if not band or band[1] < item["k"]:
            fallback.append(filt)
            continue
        edges = [None] + sorted({b for b in band[0] if b}) + [None]
        per = math.ceil(filt["offer_count"] / (len(edges) - 1))
        for i, (lo, hi) in enumerate(zip(edges, edges[1:])):
            sub = _derive_filter(filt, lo, hi - 1 if hi else None)
            sub["label"] = f"{sub['label']}#{i}"
            sub["offer_count"] = per
            result.append(sub)
        split_ok += 1

    log.info(f"plan: {split_ok} сегментов разбито по БД, {len(fallback)} в фоллбэк")

    if fallback:
        retried = await asyncio.gather(*[
            _http_maybe_split(pool, filt,
                              _deal_cfg(cfg, filt).get("max_offers", max_offers),
                              _deal_cfg(cfg, filt).get("min_split", min_split),
                              cfg, sem, sessions)
            for filt in fallback
        ])
        for sub in retried:
            result.extend(sub)

    return result


async def http_plan_filters(pool, cfg=None, url_queue=None, seen=None):
    cfg = cfg or load_scraper_config()
    raw = build_filters_from_config(cfg)
    max_offers = cfg.get("max_offers_per_filter", 1400)
    min_split = cfg.get("min_price_split", 1_000_000)

    cp = load_checkpoint("cian_plan")
    if cp and cp.get("filters"):
        total = cp.get("total_offers", 0)
        log.info(f"plan: loaded from checkpoint ({len(cp['filters'])} filters, ~{total} offers)")
        for f in cp["filters"]:
            if "otype" not in f:
                f["otype"] = None
            if "deal" not in f:
                f["deal"] = "sale"
        cp["filters"].sort(key=_priority_key)
        return cp["filters"], total

    sem = asyncio.Semaphore(cfg.get("planner_concurrency", 64))
    sessions = {}
    result = []
    early_urls = []
    lock = asyncio.Lock()

    async def worker(filt):
        sub = await _http_maybe_split(pool, filt,
                                      _deal_cfg(cfg, filt).get("max_offers", max_offers),
                                      _deal_cfg(cfg, filt).get("min_split", min_split),
                                      cfg, sem, sessions, early_urls=early_urls)
        async with lock:
            result.extend(sub)

    try:
        if cfg.get("plan_from_db", True):
            result = await _plan_from_db(pool, cfg, sem, sessions, early_urls)
        else:
            log.info(f"plan: {len(raw)} raw filters, processing via curl_cffi...")
            tasks = [worker(filt) for filt in raw]
            await asyncio.gather(*tasks)

            unknowns = [f for f in result if f.get("count_unknown")]
            if unknowns:
                log.info(f"plan: повторная проверка {len(unknowns)} фильтров без count")
                for f in unknowns:
                    f.pop("count_unknown", None)
                retried = await asyncio.gather(*[
                    _http_maybe_split(pool, f,
                                      _deal_cfg(cfg, f).get("max_offers", max_offers),
                                      _deal_cfg(cfg, f).get("min_split", min_split),
                                      cfg, sem, sessions) for f in unknowns
                ])
                result = [f for f in result if f not in unknowns]
                for sub in retried:
                    result.extend(sub)
    finally:
        for s in sessions.values():
            await s.close()

    if url_queue and early_urls:
        added = 0
        for href in early_urls:
            cid = extract_cian_id(href)
            if seen is not None and cid and cid in seen:
                continue
            if seen is not None and cid:
                seen.add(cid)
            try:
                url_queue.put_nowait(href)
                added += 1
            except asyncio.QueueFull:
                break
        if added:
            log.info(f"plan: fed {added} early URLs from page 1 into offer queue")

    result.sort(key=_priority_key)
    total_offers = sum(f.get("offer_count", 0) for f in result)
    unknown = sum(1 for f in result if f.get("count_unknown"))
    if unknown * 10 < len(result):
        save_checkpoint("cian_plan", {"filters": result, "total_offers": total_offers})
    else:
        log.warning(f"plan: {unknown}/{len(result)} фильтров без count, план не сохраняем")
    log.info(f"plan: {len(result)} filters, ~{total_offers} offers total")
    return result, total_offers


if __name__ == "__main__":
    async def _dry_run():
        from pipeline.cian.proxy_farm import build_proxy_pool
        cfg = load_scraper_config()
        pool = await build_proxy_pool(cfg)
        filters, total = await http_plan_filters(pool, cfg)
        log.info(f"total: {len(filters)} filters, ~{total} offers")
        for f in filters[:10]:
            log.info(f"  {f['label']}")
        if len(filters) > 10:
            log.info(f"  ... and {len(filters) - 10} more")

    asyncio.run(_dry_run())
