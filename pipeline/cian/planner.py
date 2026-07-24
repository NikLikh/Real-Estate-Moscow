import asyncio
import logging
import random

from config.settings import load_scraper_config
from pipeline.cian.parsers import extract_cian_id
from pipeline.cian.runtime import load_checkpoint, save_checkpoint

log = logging.getLogger("re")


def build_filters_from_config(cfg=None):
    cfg = cfg or load_scraper_config()
    filters = []

    for region_name, region in cfg["regions"].items():
        otypes = region.get("object_types") or {None: None}

        for room_name in region["rooms"]:
            for lo, hi in region["prices"]:
                for otype_name in otypes:
                    label_parts = [region_name, room_name, _price_label(lo, hi)]
                    if otype_name:
                        label_parts.append(otype_name)

                    filters.append(
                        {
                            "label": "/".join(label_parts),
                            "region": region_name,
                            "room": room_name,
                            "price_lo": lo,
                            "price_hi": hi,
                            "otype": otype_name,
                        }
                    )

    filters.sort(key=_priority_key)
    return filters


_SMALL_ROOMS = {"studio", "1-room", "2-room"}
_REGION_ORDER = {"mo": 0, "msk": 1, "spb": 2, "lo": 3}


def _priority_key(f):
    region = _REGION_ORDER.get(f["region"], 9)
    small = f["room"] in _SMALL_ROOMS
    otype = f.get("otype")

    if otype == "new" or otype is None:
        group = 0 if small else 1
    elif otype == "resale":
        group = 2 if small else 3
    else:
        group = 4

    jitter = random.random()
    return (group, region, jitter)


def _price_label(lo, hi):
    def fmt(v):
        if v is None:
            return ""
        if v >= 1_000_000:
            return f"{v / 1_000_000:g}M"
        return f"{v // 1000}K"

    return f"{fmt(lo)}-{fmt(hi) or '+'}"


def _derive_filter(parent, new_lo, new_hi):
    label_parts = [parent["region"], parent["room"], _price_label(new_lo, new_hi)]
    if parent["otype"]:
        label_parts.append(parent["otype"])

    return {
        "label": "/".join(label_parts),
        "region": parent["region"],
        "room": parent["room"],
        "price_lo": new_lo,
        "price_hi": new_hi,
        "otype": parent["otype"],
    }


async def _http_check_count(pool, filt, cfg, sem, max_retries=10):
    from curl_cffi.requests import AsyncSession
    from pipeline.cian.proxy_farm.detector import headers as _headers, is_waf as _is_waf, is_captcha as _is_captcha
    from pipeline.cian.api import api_headers, build_json_query, parse_search

    body = build_json_query(filt, cfg)
    h = api_headers(_headers())

    for attempt in range(max_retries):
        slot = await pool.acquire()
        if not slot:
            await asyncio.sleep(3)
            continue

        async with sem:
            try:
                async with AsyncSession(
                    impersonate="chrome", proxy=slot.proxy, max_clients=5
                ) as s:
                    resp = await s.post(cfg["api_listing_endpoint"], json=body, headers=h, timeout=15)
            except Exception:
                pool.report_waf(slot, 10)
                continue

            if _is_waf(resp.text, resp.status_code):
                pool.report_waf(slot, 30)
                continue

            if _is_captcha(resp.text, str(resp.url)):
                pool.report_waf(slot, 60)
                continue

            pool.report_ok(slot)
            try:
                data = resp.json()["data"]
            except Exception:
                return None, []
            count, rows = parse_search(data)
            return count, [u for u, *_ in rows]

    return None, []


async def _http_maybe_split(pool, filt, max_offers, min_split, cfg, sem, early_urls=None):
    count, page1_urls = await _http_check_count(pool, filt, cfg, sem)
    if early_urls is not None and page1_urls:
        early_urls.extend(page1_urls)
    lo = filt["price_lo"] or 0
    hi = filt["price_hi"]

    if count is None:
        # не знаем сколько, но листинг обойдёт до 54 страниц = 1512 карточек
        filt["offer_count"] = 54 * 28
        filt["count_unknown"] = True
        log.info(f"  {filt['label']}: count unknown, ставим {filt['offer_count']} (max pages)")
        return [filt]

    filt["offer_count"] = count

    if count <= max_offers:
        log.info(f"  {filt['label']}: {count} offers, ok")
        return [filt]

    if hi is None:
        hi = lo * 3 if lo else 100_000_000
    mid = (lo + hi) // 2

    if mid - lo < min_split or hi - mid < min_split:
        log.info(f"  {filt['label']}: {count} offers, дальше не дробим")
        return [filt]

    log.info(f"  {filt['label']}: {count} offers, дробим на {mid // 1_000_000}M")

    left = _derive_filter(filt, lo, mid)
    right = _derive_filter(filt, mid, filt["price_hi"])

    left_res, right_res = await asyncio.gather(
        _http_maybe_split(pool, left, max_offers, min_split, cfg, sem),
        _http_maybe_split(pool, right, max_offers, min_split, cfg, sem),
    )
    return left_res + right_res


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
        cp["filters"].sort(key=_priority_key)
        return cp["filters"], total

    log.info(f"plan: {len(raw)} raw filters, processing via curl_cffi...")

    sem = asyncio.Semaphore(cfg.get("planner_concurrency", 64))
    result = []
    early_urls = []
    lock = asyncio.Lock()

    async def worker(filt):
        sub = await _http_maybe_split(pool, filt, max_offers, min_split, cfg, sem, early_urls=early_urls)
        async with lock:
            result.extend(sub)

    tasks = [worker(filt) for filt in raw]
    await asyncio.gather(*tasks)

    unknowns = [f for f in result if f.get("count_unknown")]
    if unknowns:
        log.info(f"plan: повторная проверка {len(unknowns)} фильтров без count")
        for f in unknowns:
            f.pop("count_unknown", None)
        retried = await asyncio.gather(*[
            _http_maybe_split(pool, f, max_offers, min_split, cfg, sem) for f in unknowns
        ])
        result = [f for f in result if f not in unknowns]
        for sub in retried:
            result.extend(sub)

    # ранние URL-ы в очередь, чтобы offer workers не простаивали
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
    save_checkpoint("cian_plan", {"filters": result, "total_offers": total_offers})
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
