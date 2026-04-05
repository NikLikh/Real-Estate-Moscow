"""Adaptive filter planner -- дробит ценовые диапазоны если слишком много объявлений."""

import asyncio
import logging
import random
import re

from config.settings import load_scraper_config
from scraper.browser import detect_captcha, detect_waf_rate_limit
from scraper.runtime import (
    EndpointEvent,
    EndpointSession,
    load_checkpoint,
    save_checkpoint,
)

log = logging.getLogger("re")


def build_filters_from_config(cfg=None):
    # декартово произведение регион x комнаты x цена x тип объекта
    cfg = cfg or load_scraper_config()
    base = cfg["cian_base"]
    filters = []

    for region_name, region in cfg["regions"].items():
        region_id = region["id"]
        rooms = region["rooms"]
        prices = region["prices"]
        otypes = region.get("object_types") or {None: None}

        for room_name, room_param in rooms.items():
            for price_range in prices:
                lo, hi = price_range
                price_param = _price_param(lo, hi)

                for otype_name, otype_param in otypes.items():
                    parts = [base, f"region={region_id}", room_param, price_param]
                    if otype_param:
                        parts.append(otype_param)
                    url = "&".join(parts)

                    label_parts = [region_name, room_name, _price_label(lo, hi)]
                    if otype_name:
                        label_parts.append(otype_name)

                    filters.append(
                        {
                            "label": "/".join(label_parts),
                            "url": url,
                            "region": region_name,
                            "room": room_name,
                            "price_lo": lo,
                            "price_hi": hi,
                            "otype": otype_name,
                        }
                    )

    filters.sort(key=_priority_key)
    return filters


# studio/1-room/2-room считаем "малой" комнатностью
_SMALL_ROOMS = {"studio", "1-room", "2-room"}

# МО обрабатываем раньше МСК
_REGION_ORDER = {"mo": 0, "msk": 1}


def _priority_key(f):
    # новостройки до 2-комн -> новостройки 3+ -> вторичка до 2-комн -> вторичка 3+ -> остальное
    region = _REGION_ORDER.get(f["region"], 9)
    small = f["room"] in _SMALL_ROOMS
    otype = f.get("otype")

    if otype == "new" or otype is None:
        # МО без object_types, считаем наравне с новостройками
        group = 0 if small else 1
    elif otype == "resale":
        group = 2 if small else 3
    else:
        group = 4

    # рандом внутри группы чтобы не долбить один ценовой сегмент подряд
    jitter = random.random()
    return (group, region, jitter)


def _price_param(lo, hi):
    parts = []
    if lo:
        parts.append(f"minprice={lo}")
    if hi:
        parts.append(f"maxprice={hi}")
    return "&".join(parts)


def _price_label(lo, hi):
    def fmt(v):
        if v is None:
            return ""
        if v >= 1_000_000:
            return f"{v // 1_000_000}M"
        return f"{v // 1000}K"

    return f"{fmt(lo)}-{fmt(hi) or '+'}"


def parse_offer_count(html: str) -> int | None:
    m = re.search(r"Найдено\s+([\d\s\xa0]+)\s*объявлен", html)
    if not m:
        return None
    digits = re.sub(r"[^\d]", "", m.group(1))
    return int(digits) if digits else None


async def check_filter_count(session, url, sem):
    from scraper.browser import jittered_delay

    result, _ = await session.goto(f"{url}&p=1", sem, timeout=30000)
    if result == "network":
        await session.rotate(EndpointEvent.NETWORK, "planner network")
        return None
    if not result:
        return None

    if await detect_waf_rate_limit(session.page):
        await session.rotate(EndpointEvent.WAF, "planner waf")
        return None

    if await detect_captcha(session.page, url_only=True):
        ok = await session.handle_captcha(f"{url}&p=1")
        if not ok:
            await session.rotate(EndpointEvent.CAPTCHA, "planner captcha")
            return None

    await jittered_delay(0.8, 1.5)

    try:
        html = await session.page.content()
    except Exception:
        return None

    await session.report_success()
    return parse_offer_count(html)


async def plan_filters(browser_pool, sem, cfg=None, orchestrator=None, pw=None):
    cfg = cfg or load_scraper_config()
    raw = build_filters_from_config(cfg)
    max_offers = cfg.get("max_offers_per_filter", 1400)
    min_split = cfg.get("min_price_split", 1_000_000)

    cp = load_checkpoint("cian_plan")
    if cp and cp.get("filters"):
        log.info(f"plan: loaded from checkpoint ({len(cp['filters'])} filters)")
        # пересортируем по новым приоритетам
        for f in cp["filters"]:
            if "otype" not in f:
                f["otype"] = None
        cp["filters"].sort(key=_priority_key)
        return cp["filters"]

    # planner шарит endpoint (shared=True) -- не зависит от healthy_count
    n_pages = min(len(browser_pool.all), max(1, cfg.get("planner_workers", 3)))
    sessions = []
    if orchestrator:
        for i in range(n_pages):
            session = EndpointSession(
                f"PLN{i+1}",
                "planner",
                browser_pool.all[i % len(browser_pool.all)],
                orchestrator,
                block_extra=True,
                pw=pw,
                cfg=cfg,
                do_warmup=False,
                shared=True,
            )
            await session.open()
            sessions.append(session)

    log.info(f"plan: {len(raw)} raw filters, {n_pages} parallel workers")

    # очередь фильтров для проверки
    queue = asyncio.Queue()
    for filt in raw:
        await queue.put(filt)

    result = []
    lock = asyncio.Lock()

    async def worker(session, wid):
        while True:
            try:
                filt = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            sub = await _maybe_split(
                session, sem, filt, max_offers, min_split, cfg, queue
            )
            async with lock:
                result.extend(sub)

    try:
        if orchestrator:
            await asyncio.gather(*[worker(sessions[i], i) for i in range(n_pages)])
        else:
            raise RuntimeError("planner requires endpoint orchestrator")
    finally:
        for session in sessions:
            await session.close()

    result.sort(key=_priority_key)
    save_checkpoint("cian_plan", {"filters": result})
    log.info(f"plan: {len(result)} filters (from {len(raw)} raw)")
    return result


async def _maybe_split(session, sem, filt, max_offers, min_split, cfg, queue=None):
    count = await check_filter_count(session, filt["url"], sem)
    lo = filt["price_lo"] or 0
    hi = filt["price_hi"]

    if count is None:
        log.info(f"  {filt['label']}: count unknown, keep as is")
        return [filt]

    if count <= max_offers:
        log.info(f"  {filt['label']}: {count} offers -- ok")
        return [filt]

    if hi is None:
        hi = lo * 3 if lo else 100_000_000
    mid = (lo + hi) // 2

    if mid - lo < min_split or hi - mid < min_split:
        log.info(f"  {filt['label']}: {count} offers -- can't split further")
        return [filt]

    log.info(f"  {filt['label']}: {count} offers -- splitting at {mid // 1_000_000}M")

    left = _derive_filter(filt, lo, mid, cfg)
    right = _derive_filter(filt, mid, hi, cfg)

    # splits проверяем рекурсивно на той же page
    result = []
    for sub in [left, right]:
        result.extend(await _maybe_split(session, sem, sub, max_offers, min_split, cfg))
    return result


def _derive_filter(parent, new_lo, new_hi, cfg):
    base = cfg["cian_base"]
    region = cfg["regions"][parent["region"]]
    room_param = region["rooms"][parent["room"]]
    price_param = _price_param(new_lo, new_hi)

    parts = [base, f"region={region['id']}", room_param, price_param]

    otypes = region.get("object_types") or {}
    otype_param = otypes.get(parent["otype"])
    if otype_param:
        parts.append(otype_param)

    label_parts = [parent["region"], parent["room"], _price_label(new_lo, new_hi)]
    if parent["otype"]:
        label_parts.append(parent["otype"])

    return {
        "label": "/".join(label_parts),
        "url": "&".join(parts),
        "region": parent["region"],
        "room": parent["room"],
        "price_lo": new_lo,
        "price_hi": new_hi,
        "otype": parent["otype"],
    }


async def _http_check_count(pool, url, cfg):
    """curl_cffi версия check_filter_count -- без browser"""
    from curl_cffi.requests import AsyncSession
    from scraper.http_offers import _headers, _is_waf

    slot = await pool.acquire()
    if not slot:
        return None

    try:
        async with AsyncSession(
            impersonate="chrome", proxy=slot.proxy, max_clients=5
        ) as s:
            resp = await s.get(f"{url}&p=1", headers=_headers(), timeout=15)
    except Exception:
        return None

    if _is_waf(resp.text, resp.status_code):
        pool.report_waf(slot, 30)
        return None

    pool.report_ok(slot)
    return parse_offer_count(resp.text)


async def _http_maybe_split(pool, filt, max_offers, min_split, cfg):
    count = await _http_check_count(pool, filt["url"], cfg)
    lo = filt["price_lo"] or 0
    hi = filt["price_hi"]

    if count is None:
        log.info(f"  {filt['label']}: count unknown, keep as is")
        return [filt]

    if count <= max_offers:
        log.info(f"  {filt['label']}: {count} offers -- ok")
        return [filt]

    if hi is None:
        hi = lo * 3 if lo else 100_000_000
    mid = (lo + hi) // 2

    if mid - lo < min_split or hi - mid < min_split:
        log.info(f"  {filt['label']}: {count} offers -- can't split further")
        return [filt]

    log.info(f"  {filt['label']}: {count} offers -- splitting at {mid // 1_000_000}M")

    left = _derive_filter(filt, lo, mid, cfg)
    right = _derive_filter(filt, mid, hi, cfg)

    result = []
    for sub in [left, right]:
        result.extend(await _http_maybe_split(pool, sub, max_offers, min_split, cfg))
    return result


async def http_plan_filters(pool, cfg=None):
    """planner через curl_cffi -- без browser, использует HttpPool"""
    cfg = cfg or load_scraper_config()
    raw = build_filters_from_config(cfg)
    max_offers = cfg.get("max_offers_per_filter", 1400)
    min_split = cfg.get("min_price_split", 1_000_000)

    cp = load_checkpoint("cian_plan")
    if cp and cp.get("filters"):
        log.info(f"plan: loaded from checkpoint ({len(cp['filters'])} filters)")
        for f in cp["filters"]:
            if "otype" not in f:
                f["otype"] = None
        cp["filters"].sort(key=_priority_key)
        return cp["filters"]

    log.info(f"plan: {len(raw)} raw filters, processing via curl_cffi...")

    sem = asyncio.Semaphore(10)
    result = []
    lock = asyncio.Lock()

    async def worker(filt):
        sub = await _http_maybe_split(pool, filt, max_offers, min_split, cfg)
        async with lock:
            result.extend(sub)

    # параллельно, но не больше 10 одновременно
    tasks = []
    for filt in raw:
        tasks.append(worker(filt))
    await asyncio.gather(*tasks)

    result.sort(key=_priority_key)
    save_checkpoint("cian_plan", {"filters": result})
    log.info(f"plan: {len(result)} filters (from {len(raw)} raw)")
    return result


async def _dry_run():
    from patchright.async_api import async_playwright

    from scraper.browser import launch_browser_pool
    from scraper.proxy import resolve_runtime_endpoints
    from scraper.runtime import EndpointOrchestrator, EndpointRegistry

    cfg = load_scraper_config()
    sem = asyncio.Semaphore(4)
    await resolve_runtime_endpoints(cfg)
    registry = EndpointRegistry(cfg.get("verified_endpoints", []))
    orchestrator = EndpointOrchestrator(registry, cfg)

    async with async_playwright() as pw:
        pool = await launch_browser_pool(pw, 4, headless=True)
        try:
            filters = await plan_filters(
                pool, sem, cfg, orchestrator=orchestrator, pw=pw
            )
            log.info(f"total: {len(filters)} filters")
            for f in filters[:10]:
                log.info(f"  {f['label']}")
            if len(filters) > 10:
                log.info(f"  ... and {len(filters) - 10} more")
        finally:
            await pool.close_all()


if __name__ == "__main__":
    asyncio.run(_dry_run())
