"""Скрапер наш.дом.рф

Headed hidden browser (--window-position off-screen): ServicePipe блокирует headless.
Phase 1: listing --N parallel workers, __NEXT_DATA__ JSON extraction.
Phase 2: offers --N parallel workers, CDP blocking, parse_domrf_offer(html).
"""

import asyncio
import json
import logging
import random
import re
import time
from datetime import datetime
from urllib.parse import quote

from patchright.async_api import async_playwright

from config.settings import load_scraper_config
from db.repository import get_cached_urls, save_rows
from scraper.browser import apply_cdp_blocking, jittered_delay, launch_domrf_browser
from scraper.parsers_domrf import parse_domrf_offer
from scraper.runtime import (
    clear_checkpoint,
    install_shutdown_handler,
    is_shutting_down,
    load_checkpoint,
    save_checkpoint,
    should_stop,
)

log = logging.getLogger("re")

BASE_URL = "https://xn--80az8a.xn--d1aqf.xn--p1ai"
LISTING_PATH = (
    "/%D1%81%D0%B5%D1%80%D0%B2%D0%B8%D1%81%D1%8B/"
    "%D0%BA%D0%B0%D1%82%D0%B0%D0%BB%D0%BE%D0%B3-%D0%BA%D0%B2%D0%B0%D1%80%D1%82%D0%B8%D1%80/"
    "%D1%81%D0%BF%D0%B8%D1%81%D0%BE%D0%BA"
)

# offer pages: CSS/images/fonts не нужны, JS оставляем для SP challenge
OFFER_BLOCK_EXTRA = [
    "**/*.css",
    "**/*.svg",
    "**/*.jpg",
    "**/*.jpeg",
    "**/*.png",
    "**/*.webp",
    "**/*.gif",
    "**/*.woff2",
    "**/*.woff",
]

_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    re.DOTALL,
)


def _domrf_cfg(cfg):
    return cfg.get("domrf", {})


class ProxyPool:
    """Round-robin пул прокси, потокобезопасный для asyncio."""

    def __init__(self, proxies: list):
        self._proxies = proxies if proxies else [None]
        self._idx = 0
        self._lock = asyncio.Lock()
        # только один worker пересоздаёт context одновременно
        self.recycle_lock = asyncio.Lock()

    async def next(self):
        async with self._lock:
            px = self._proxies[self._idx % len(self._proxies)]
            self._idx += 1
            return px

    def __len__(self):
        return len(self._proxies)


def _discover_cyberghost_proxies():
    # статический server_list.json внутри CRX -> HTTPS proxies
    from pathlib import Path

    server_list = (
        Path(__file__).resolve().parent.parent
        / "extensions"
        / "cyberghost"
        / "assets"
        / "server_list.json"
    )
    if not server_list.exists():
        return []
    try:
        with open(server_list) as f:
            locations = json.load(f)
        proxies = []
        for loc in locations:
            for node in loc.get("nodes", []):
                dns = node.get("dnsname")
                if dns:
                    proxies.append(f"https://{dns}")
        return proxies
    except Exception:
        return []


def _get_proxies(cfg):
    dc = _domrf_cfg(cfg)
    manual = dc.get("proxies", [])
    auto = dc.get("auto_discover_proxies", True)

    all_proxies = []
    if auto:
        cg = _discover_cyberghost_proxies()
        if cg:
            log.info(f"[PROXY] CyberGhost: {len(cg)} servers")
            all_proxies.extend(cg)
    all_proxies.extend(manual)

    if not all_proxies:
        return [None]  # direct IP
    return [{"server": p} if isinstance(p, str) else p for p in all_proxies]


def build_listing_urls(cfg):
    dc = _domrf_cfg(cfg)
    base = dc.get("base_url", BASE_URL)
    path = dc.get("listing_path", LISTING_PATH)
    flat_status = dc.get("flat_status", "free,booked")
    regions = dc.get(
        "regions",
        {
            "moscow": {"place": "0-1", "max_pages": 500},
            "mo": {"place": "50", "max_pages": 500},
        },
    )

    result = []
    for name, rcfg in regions.items():
        tpl = (
            f"{base}{path}"
            f"?flatStatus={quote(flat_status, safe=',')}"
            f"&place={rcfg['place']}"
            "&page={page}"
        )
        result.append((name, tpl, rcfg.get("max_pages", 500)))
    return result


async def _create_context(browser, proxy=None, storage_state=None):
    # НЕ override UA -- ServicePipe детектит mismatch с реальной версией Chromium
    kw = dict(
        viewport={"width": 1920, "height": 1080},
        locale="ru-RU",
        timezone_id="Europe/Moscow",
    )
    if proxy:
        kw["proxy"] = proxy
    if storage_state:
        kw["storage_state"] = storage_state
    return await browser.new_context(**kw)


async def _wait_for_sp(page, timeout=30):
    # ServicePipe JS challenge ~3-5s, без JS не пройти
    for i in range(timeout):
        try:
            html = await page.content()
        except Exception:
            await asyncio.sleep(1)
            continue

        html_lower = html.lower()
        url = page.url

        if "__NEXT_DATA__" in html:
            log.info(f"[SP] passed in {i}s (__NEXT_DATA__)")
            return True

        sp_present = (
            "servicepipe" in html_lower
            or "/xpvnsulc" in url
            or "just a moment" in html_lower
            or (len(html) < 3000 and "<noscript>" in html_lower)
        )

        if not sp_present and len(html) > 10000:
            log.info(f"[SP] passed in {i}s ({len(html)}B)")
            return True

        title = await page.title()
        if "403" in title:
            if len(html) < 5000:
                log.warning(f"[SP] blocked 403 at {i}s")
                return False
            # domrf 403 (не SP) --cookies уже установлены
            log.info(f"[SP] domrf 403, cookies set, {i}s")
            return True

        if i % 5 == 0:
            log.info(f"[SP] {i}/{timeout}s html={len(html)}B sp={sp_present}")

        await asyncio.sleep(1)

    # последняя попытка --reload
    log.info("[SP] timeout, reload...")
    try:
        await page.reload(timeout=30000, wait_until="domcontentloaded")
        await asyncio.sleep(5)
        html = await page.content()
        if "__NEXT_DATA__" in html or len(html) > 10000:
            log.info(f"[SP] passed after reload ({len(html)}B)")
            return True
    except Exception:
        pass
    return False


def _parse_next_data_flats(html):
    m = _NEXT_DATA_RE.search(html)
    if not m:
        return []
    try:
        nd = json.loads(m.group(1))
        return nd.get("props", {}).get("pageProps", {}).get("flats", [])
    except (json.JSONDecodeError, KeyError):
        return []


def _flat_url(elem_id):
    return f"{BASE_URL}/сервисы/каталог-квартир/квартира/{elem_id}"


async def _listing_worker(name, browser, url_tpl, pages, result_urls, seen, cfg, pool):
    proxy = await pool.next()
    ctx = await _create_context(browser, proxy=proxy)
    page = await ctx.new_page()
    sp_passed = False
    consecutive_fails = 0  # network/proxy ошибки -- ротация
    real_empty = 0  # страницы с 0 flats при валидном ответе -- конец данных
    rotations = 0
    max_rotations = 8  # не крутить бесконечно

    async def _rotate():
        nonlocal ctx, page, proxy, sp_passed, consecutive_fails, rotations
        async with pool.recycle_lock:
            try:
                await page.close()
                await ctx.close()
            except Exception:
                pass
            proxy = await pool.next()
            try:
                ctx = await _create_context(browser, proxy=proxy)
                page = await ctx.new_page()
            except Exception as e:
                log.error(f"[{name}] browser dead, stopping: {e!r:.80}")
                raise
        sp_passed = False
        consecutive_fails = 0
        rotations += 1
        log.info(f"[{name}] rotated ({rotations}) -> {proxy and proxy['server']}")

    try:
        for pg in pages:
            if should_stop():
                break

            url = url_tpl.format(page=pg)
            is_first = not sp_passed

            loaded = False
            for attempt in range(3):
                try:
                    await page.goto(url, timeout=60000, wait_until="domcontentloaded")
                    loaded = True
                    break
                except Exception as e:
                    if attempt < 2:
                        log.warning(f"[{name}] p.{pg} retry {attempt + 1}: {e!s:.80s}")
                        await asyncio.sleep(5 * (attempt + 1))

            if not loaded:
                consecutive_fails += 1
                if consecutive_fails >= 3:
                    if rotations >= max_rotations:
                        log.warning(f"[{name}] max rotations, stopping")
                        break
                    await _rotate()
                continue

            # SP challenge на первой странице
            if is_first:
                if not await _wait_for_sp(page):
                    log.warning(f"[{name}] SP failed, rotating")
                    if rotations < max_rotations:
                        await _rotate()
                    continue
                sp_passed = True
                log.info(f"[{name}] SP passed")
                if "__NEXT_DATA__" not in (await page.content()):
                    try:
                        await page.goto(
                            url, timeout=60000, wait_until="domcontentloaded"
                        )
                        await asyncio.sleep(3)
                    except Exception:
                        pass

            html = None
            for _ in range(8):
                html = await page.content()
                if "__NEXT_DATA__" in html:
                    break
                await asyncio.sleep(1)

            if not html or "__NEXT_DATA__" not in html:
                consecutive_fails += 1
                if consecutive_fails >= 3:
                    if rotations < max_rotations:
                        await _rotate()
                    else:
                        log.warning(f"[{name}] max rotations, stopping")
                        break
                continue

            consecutive_fails = 0  # страница загрузилась нормально

            flats = _parse_next_data_flats(html)
            if not flats:
                real_empty += 1
                if real_empty >= 3:
                    log.info(f"[{name}] 3 real empty pages, end of data")
                    break
                continue

            real_empty = 0  # сброс: были данные

            consecutive_empty = 0
            new = 0
            for f in flats:
                flat_url = _flat_url(f["elemId"])
                if flat_url not in seen:
                    seen.add(flat_url)
                    result_urls.append(flat_url)
                    new += 1

            log.info(
                f"[{name}] p.{pg}: {len(flats)} flats, {new} new, total {len(result_urls)}"
            )
            ld = _domrf_cfg(cfg).get("listing_delay", [0.3, 0.8])
            await jittered_delay(ld[0], ld[1])
    finally:
        await page.close()
        await ctx.close()


async def crawl_listings(browser, cfg, seen):
    """Listing: N parallel workers, interleaved pages.

    page=0 --landing (no data), start from page=1.
    Each worker passes SP independently on its first page.
    """
    dc = _domrf_cfg(cfg)
    n_workers = dc.get("listing_workers", 4)
    proxies = _get_proxies(cfg)
    pool = ProxyPool(proxies)
    regions = build_listing_urls(cfg)
    all_urls = []

    for region_name, url_tpl, max_pages in regions:
        if should_stop():
            break

        page_ranges = [
            list(range(i + 1, max_pages + 1, n_workers)) for i in range(n_workers)
        ]

        log.info(
            f"[LISTING] {region_name}: {max_pages} pages, "
            f"{n_workers} workers, {len(proxies)} IPs"
        )

        tasks = []
        for i in range(n_workers):
            if not page_ranges[i]:
                continue
            tasks.append(
                _listing_worker(
                    f"L{i + 1}:{region_name}",
                    browser,
                    url_tpl,
                    page_ranges[i],
                    all_urls,
                    seen,
                    cfg,
                    pool=pool,
                )
            )
        await asyncio.gather(*tasks)
        log.info(f"[LISTING] {region_name} done: {len(all_urls)} URLs")

    # кэшируем URL сразу -- если warmup упадёт, при рестарте пойдём в offers
    if all_urls:
        save_checkpoint("domrf", {"pending_urls": all_urls})
        log.info(f"[LISTING] checkpoint saved: {len(all_urls)} URLs")

    # storage_state для offer workers: один warmup request
    # через прокси -- голый IP может быть заблокирован
    warmup_url = regions[0][1].format(page=1)
    storage = None
    for attempt, px in enumerate(proxies[:5] if proxies else [None]):
        ctx = await _create_context(browser, proxy=px)
        page = await ctx.new_page()
        try:
            await page.goto(warmup_url, timeout=30000, wait_until="domcontentloaded")
            await _wait_for_sp(page)
            storage = await ctx.storage_state()
            log.info(f"[WARMUP] ok via {px and px['server']}")
            break
        except Exception as e:
            log.warning(
                f"[WARMUP] attempt {attempt + 1} failed ({px and px['server']}): {e!r:.120}"
            )
        finally:
            await page.close()
            await ctx.close()
    if not storage:
        log.warning("[WARMUP] all failed -- workers will pass SP individually")

    return all_urls, storage


async def _offer_worker(name, browser, urls, stats, storage_state, cfg, pool):
    dc = _domrf_cfg(cfg)
    base_batch = dc.get("batch_size", 30)
    batch_size = base_batch + random.randint(-5, 10)
    batch = []
    consecutive_errors = 0
    max_consecutive = 3

    proxy = await pool.next()
    ctx = await _create_context(browser, proxy=proxy, storage_state=storage_state)
    page = await ctx.new_page()
    await apply_cdp_blocking(page, extra_patterns=OFFER_BLOCK_EXTRA)
    req_count = 0

    async def _flush():
        nonlocal batch
        if not batch:
            return
        saved = save_rows(batch)
        stats["saved"] += saved
        if saved:
            log.info(f"[{name}] DB +{saved}")
        batch = []

    async def _recycle(rotate=False):
        nonlocal ctx, page, req_count, proxy, batch_size
        await _flush()
        # Lock: только один worker пересоздаёт context за раз
        async with pool.recycle_lock:
            try:
                await page.close()
                await ctx.close()
            except Exception:
                pass
            if rotate:
                proxy = await pool.next()
            try:
                ctx = await _create_context(
                    browser, proxy=proxy, storage_state=storage_state
                )
                page = await ctx.new_page()
                await apply_cdp_blocking(page, extra_patterns=OFFER_BLOCK_EXTRA)
            except Exception as e:
                log.error(f"[{name}] browser dead, stopping: {e!r:.80}")
                raise
        req_count = 0
        batch_size = base_batch + random.randint(-5, 10)  # новый jitter
        if rotate:
            log.info(f"[{name}] rotated -> {proxy and proxy['server']}")

    try:
        for i, url in enumerate(urls):
            if should_stop():
                break

            req_count += 1
            if req_count > batch_size:
                await _recycle(rotate=True)

            is_first = req_count == 1
            try:
                await page.goto(
                    url,
                    timeout=60000 if is_first else 20000,
                    wait_until="domcontentloaded",
                )
            except Exception as e:
                stats["errors"] += 1
                consecutive_errors += 1
                if stats["errors"] <= 5 or stats["errors"] % 20 == 0:
                    log.warning(f"[{name}] goto err #{stats['errors']}: {e!r:.120}")
                if consecutive_errors >= max_consecutive:
                    await _recycle(rotate=True)
                continue

            if is_first:
                if not await _wait_for_sp(page, timeout=15):
                    try:
                        await page.reload(timeout=30000, wait_until="domcontentloaded")
                        await _wait_for_sp(page, timeout=10)
                    except Exception:
                        pass
                log.info(f"[{name}] first page loaded, starting parse")

            try:
                await page.wait_for_selector(
                    '[class*="PriceBlock"], [class*="Characteristics"]',
                    timeout=10000,
                )
            except Exception:
                stats["errors"] += 1
                consecutive_errors += 1
                if stats["errors"] <= 5 or stats["errors"] % 20 == 0:
                    title = await page.title()
                    log.warning(
                        f"[{name}] selector timeout #{stats['errors']}: "
                        f"title='{title}' url=...{url[-50:]}"
                    )
                if consecutive_errors >= max_consecutive:
                    await _recycle(rotate=True)
                continue

            consecutive_errors = 0  # успех -- сбрасываем счётчик

            try:
                html = await page.content()
            except Exception as e:
                stats["errors"] += 1
                log.warning(f"[{name}] content err: {e!r:.80}")
                continue

            data = parse_domrf_offer(html)
            data["url"] = url
            data["source"] = "domrf"
            data["is_new_building"] = True
            data["parsed_at"] = datetime.now().isoformat()

            if i < 2:
                log.info(
                    f"[{name}] DIAG url={url[-50:]} "
                    f"price={data.get('price')} rooms={data.get('rooms')} "
                    f"complex={data.get('residential_complex')}"
                )

            if not data.get("price"):
                stats["no_price"] += 1
                continue

            batch.append(data)
            stats["parsed"] += 1

            if len(batch) >= 20:
                await _flush()

            od = _domrf_cfg(cfg).get("offer_delay", [0.5, 1.0])
            await jittered_delay(od[0], od[1])

            if (i + 1) % 10 == 0:
                log.info(
                    f"[{name}] {i + 1}/{len(urls)} "
                    f"parsed={stats['parsed']} err={stats['errors']} noprice={stats['no_price']}"
                )
    finally:
        await _flush()
        try:
            await page.close()
            await ctx.close()
        except Exception:
            pass


async def _run_session(cfg):
    dc = _domrf_cfg(cfg)
    n_offer = dc.get("offer_workers", 4)
    proxies = _get_proxies(cfg)

    seen = get_cached_urls(["domrf"])
    log.info(f"cache: {len(seen)} urls, proxies: {len(proxies)} IPs")

    checkpoint = load_checkpoint("domrf") or {}
    pending = checkpoint.get("pending_urls", [])

    async with async_playwright() as pw:
        browser = await launch_domrf_browser(pw)

        try:
            # Phase 1: listing
            if pending:
                log.info(f"[RESUME] {len(pending)} pending from checkpoint")
                all_urls = [u for u in pending if u not in seen]
                # warmup для storage_state (через прокси, с retry)
                warmup_url = build_listing_urls(cfg)[0][1].format(page=1)
                storage = None
                for attempt, px in enumerate(proxies[:5] if proxies else [None]):
                    ctx = await _create_context(browser, proxy=px)
                    page = await ctx.new_page()
                    try:
                        await page.goto(
                            warmup_url, timeout=30000, wait_until="domcontentloaded"
                        )
                        await _wait_for_sp(page)
                        storage = await ctx.storage_state()
                        log.info(f"[WARMUP] ok via {px and px['server']}")
                        break
                    except Exception as e:
                        log.warning(
                            f"[WARMUP] attempt {attempt + 1} failed: {e!r:.120}"
                        )
                    finally:
                        await page.close()
                        await ctx.close()
                if not storage:
                    log.warning(
                        "[WARMUP] all failed -- workers will pass SP individually"
                    )
            else:
                all_urls, storage = await crawl_listings(browser, cfg, seen)

            if not all_urls:
                log.info("no new URLs")
                return

            log.info(f"[OFFERS] {len(all_urls)} URLs, {n_offer} workers")
            save_checkpoint("domrf", {"pending_urls": all_urls})

            # Phase 2: offers
            offer_pool = ProxyPool(proxies)
            stats = {"parsed": 0, "saved": 0, "errors": 0, "no_price": 0}
            chunks = [all_urls[i::n_offer] for i in range(n_offer)]
            tasks = []
            for i, chunk in enumerate(chunks):
                if not chunk:
                    continue
                tasks.append(
                    asyncio.create_task(
                        _offer_worker(
                            f"W{i + 1}",
                            browser,
                            chunk,
                            stats,
                            storage,
                            cfg,
                            pool=offer_pool,
                        )
                    )
                )

            async def _checkpoint_loop():
                while not should_stop():
                    save_checkpoint("domrf", {"pending_urls": all_urls, "stats": stats})
                    await asyncio.sleep(30)

            cp_task = asyncio.create_task(_checkpoint_loop())
            try:
                await asyncio.gather(*tasks)
            finally:
                cp_task.cancel()
                try:
                    await cp_task
                except asyncio.CancelledError:
                    pass

        finally:
            await browser.close()

    return stats


async def main():
    t0 = time.monotonic()
    cfg = load_scraper_config()
    install_shutdown_handler()

    stats = await _run_session(cfg)

    if not should_stop():
        clear_checkpoint("domrf")

    elapsed = (time.monotonic() - t0) / 60
    if stats:
        rate = stats["parsed"] / elapsed if elapsed > 0 else 0
        log.info(f"\n{'=' * 50}")
        log.info(f"  {'STOPPED' if is_shutting_down() else 'DONE'}")
        log.info(f"  parsed:   {stats['parsed']}")
        log.info(f"  saved:    {stats['saved']}")
        log.info(f"  errors:   {stats['errors']}")
        log.info(f"  no_price: {stats['no_price']}")
        log.info(f"  time:     {elapsed:.1f}min")
        log.info(f"  rate:     {rate:.1f}/min")
        log.info(f"{'=' * 50}")
    if is_shutting_down():
        log.info("will resume from checkpoint")


if __name__ == "__main__":
    asyncio.run(main())
