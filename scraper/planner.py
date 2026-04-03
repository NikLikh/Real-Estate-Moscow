"""Adaptive filter planner -- дробит ценовые диапазоны если слишком много объявлений."""

import asyncio
import logging
import random
import re

from config.settings import load_scraper_config
from scraper.runtime import load_checkpoint, save_checkpoint

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


async def check_filter_count(page, url, sem):
    from scraper.browser import jittered_delay

    async with sem:
        try:
            await page.goto(f"{url}&p=1", timeout=30000, wait_until="domcontentloaded")
        except Exception:
            return None

    await jittered_delay(0.8, 1.5)

    try:
        html = await page.content()
    except Exception:
        return None

    return parse_offer_count(html)


async def plan_filters(browser_pool, sem, cfg=None, proxy_pool=None):
    from scraper.browser import create_stealth_context, apply_cdp_blocking, OFFER_EXTRA_BLOCKED

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

    # создаём N страниц для параллельной проверки
    n_pages = min(len(browser_pool.all), 6)
    pages = []
    contexts = []
    for i in range(n_pages):
        browser = browser_pool.all[i % len(browser_pool.all)]
        ep = proxy_pool.get_endpoint() if proxy_pool else None
        proxy = ep.get("proxy") if ep else None
        ctx = await create_stealth_context(browser, proxy=proxy)
        page = await ctx.new_page()
        await apply_cdp_blocking(page, extra_patterns=OFFER_EXTRA_BLOCKED)
        pages.append(page)
        contexts.append(ctx)

    log.info(f"plan: {len(raw)} raw filters, {n_pages} parallel workers")

    # очередь фильтров для проверки
    queue = asyncio.Queue()
    for filt in raw:
        await queue.put(filt)

    result = []
    lock = asyncio.Lock()

    async def worker(page, wid):
        while True:
            try:
                filt = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            sub = await _maybe_split(page, sem, filt, max_offers, min_split, cfg, queue)
            async with lock:
                result.extend(sub)

    await asyncio.gather(*[worker(pages[i], i) for i in range(n_pages)])

    # закрываем план-контексты
    for page, ctx in zip(pages, contexts):
        try:
            await page.close()
            await ctx.close()
        except Exception:
            pass

    result.sort(key=_priority_key)
    save_checkpoint("cian_plan", {"filters": result})
    log.info(f"plan: {len(result)} filters (from {len(raw)} raw)")
    return result


async def _maybe_split(page, sem, filt, max_offers, min_split, cfg, queue=None):
    count = await check_filter_count(page, filt["url"], sem)
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
        result.extend(await _maybe_split(page, sem, sub, max_offers, min_split, cfg))
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


async def _dry_run():
    from patchright.async_api import async_playwright

    from scraper.browser import launch_browser_pool

    cfg = load_scraper_config()
    sem = asyncio.Semaphore(4)

    async with async_playwright() as pw:
        pool = await launch_browser_pool(pw, 4, headless=True)
        try:
            filters = await plan_filters(pool, sem, cfg)
            log.info(f"total: {len(filters)} filters")
            for f in filters[:10]:
                log.info(f"  {f['label']}")
            if len(filters) > 10:
                log.info(f"  ... and {len(filters) - 10} more")
        finally:
            await pool.close_all()


if __name__ == "__main__":
    asyncio.run(_dry_run())
