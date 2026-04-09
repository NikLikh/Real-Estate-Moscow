import asyncio
import logging
import re
import time

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

from scraper.parsers import extract_cian_id, extract_region_id, parse_offer_page, parse_offer_from_json, parse_similar_urls_from_html
from scraper.runtime import should_stop
from proxy_farm.detector import is_waf, is_captcha, is_vpn_block, headers


def _cid(href):
    """извлекаем cian_id из любого варианта URL"""
    m = re.search(r'/flat/(\d+)', href)
    return int(m.group(1)) if m else None
from proxy_farm.refresher import run_refresher

log = logging.getLogger("re")


async def fetch_offer(session, url, slot, pool, stats, cfg):
    # ждём concurrent-слот для этого прокси (ограничивает нагрузку на IP)
    await pool.acquire_concurrent(slot)
    t_fetch = time.monotonic()
    try:
        resp = await session.get(
            url,
            headers=headers(),
            timeout=cfg.get("http_timeout", 15),
            allow_redirects=True,
        )
    except Exception as e:
        pool.release_concurrent(slot)
        log.debug(f"[HTTP] {slot.label} network error: {e}")
        stats["net_errors"] = stats.get("net_errors", 0) + 1
        return None
    pool.release_concurrent(slot)
    fetch_ms = (time.monotonic() - t_fetch) * 1000
    stats["fetch_ms_total"] = stats.get("fetch_ms_total", 0) + fetch_ms
    stats["fetch_count"] = stats.get("fetch_count", 0) + 1

    html = resp.text
    if len(html) > 5_000_000:
        stats["html_too_large"] = stats.get("html_too_large", 0) + 1
        return "skipped"
    if slot.budget and slot.reqs >= slot.budget:
        pool.report_budget(slot, cfg.get("http_budget_cooldown", 300))

    if is_waf(html, resp.status_code):
        pool.report_waf(slot, cfg.get("http_waf_cooldown", 30))
        stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
        return None

    if is_captcha(html, str(resp.url)):
        pool.report_waf(slot, cfg.get("http_captcha_cooldown", 60))
        stats["captchas"] = stats.get("captchas", 0) + 1
        return None

    if is_vpn_block(html):
        pool.report_waf(slot, 60)
        stats["vpn_blocks"] = stats.get("vpn_blocks", 0) + 1
        return None

    if resp.status_code != 200:
        stats["bad_status"] = stats.get("bad_status", 0) + 1
        # 404/410 = объявление удалено, ретраить бессмысленно
        if resp.status_code in (404, 410):
            return "skipped"
        return None

    pool.report_ok(slot)

    # комнаты и доли пропускаем, не нужен BS4
    html_lower = html.lower()
    if "комната в " in html_lower or "продается доля" in html_lower:
        stats["skipped"] = stats.get("skipped", 0) + 1
        return "skipped"

    loop = asyncio.get_event_loop()

    # JSON за 1-2ms, fallback на BS4 за ~800ms
    def _timed_parse(h):
        t = time.monotonic()
        data, hist = parse_offer_from_json(h)
        if data and data.get("price"):
            ms = (time.monotonic() - t) * 1000
            return (data, hist), ms, "json"
        t2 = time.monotonic()
        res = parse_offer_page(h)
        ms = (time.monotonic() - t2) * 1000
        return res, ms, "bs4"

    (data, price_history), parse_ms, method = await loop.run_in_executor(None, _timed_parse, html)
    stats["parse_ms_total"] = stats.get("parse_ms_total", 0) + parse_ms
    stats["parse_count"] = stats.get("parse_count", 0) + 1
    if method == "json":
        stats["json_hits"] = stats.get("json_hits", 0) + 1

    # нет цены ни в JSON ни в BS4, ретраить бессмысленно
    if not data.get("price"):
        stats["no_price"] = stats.get("no_price", 0) + 1
        return "skipped"

    clean_url = url.split("?")[0]
    data["url"] = clean_url
    data["cian_id"] = extract_cian_id(clean_url)
    if not data["cian_id"]:
        stats["no_cian_id"] = stats.get("no_cian_id", 0) + 1
        return "skipped"

    allowed = cfg.get("allowed_region_ids")
    region_id = extract_region_id(html)
    if allowed and region_id and region_id not in allowed:
        stats["region_skip"] = stats.get("region_skip", 0) + 1
        return "skipped"

    data["_html_price_history"] = price_history

    similar = parse_similar_urls_from_html(html) if not allowed or not region_id or region_id in allowed else []

    timings = {"worker": slot.label, "fetch_dt": 0}
    return [data], timings, similar


async def http_offer_worker(
    name, url_queue, retry_queue, row_queue, pool, stats, cfg,
    cookies=None, seen=None, shared_sessions=None,
):
    sessions = shared_sessions if shared_sessions is not None else {}

    async def get_session(proxy):
        if proxy not in sessions:
            sessions[proxy] = AsyncSession(
                impersonate="chrome",
                proxy=proxy,
                max_clients=50,
            )
        return sessions[proxy]

    skip_urls = cfg.get("skip_url_parts", [])
    skip_urls_set = set(skip_urls)
    seen_urls = seen if seen is not None else set()
    batch_size = cfg.get("http_offer_batch", 1)

    try:
        while True:
            # набираем batch
            batch = []
            stop = False
            for _ in range(batch_size):
                try:
                    url = url_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if url is None:
                    stop = True
                    break
                if not any(part in url for part in skip_urls):
                    batch.append(url)
                else:
                    stats["skipped"] = stats.get("skipped", 0) + 1

            if not batch and not stop:
                try:
                    url = await asyncio.wait_for(url_queue.get(), timeout=60)
                except asyncio.TimeoutError:
                    if should_stop():
                        break
                    continue
                if url is None:
                    break
                if any(part in url for part in skip_urls):
                    stats["skipped"] = stats.get("skipped", 0) + 1
                    continue
                batch = [url]

            if stop or not batch:
                break

            t_cycle = time.monotonic()

            # acquire слоты, запускаем fetch параллельно
            # per-proxy concurrent ограничен в pool.acquire_concurrent()
            pending = []
            for url in batch:
                slot = await pool.acquire()
                if not slot:
                    await retry_queue.put(url)
                    continue
                session = await get_session(slot.proxy)
                pending.append((url, fetch_offer(session, url, slot, pool, stats, cfg)))

            if pending:
                results = await asyncio.gather(
                    *[coro for _, coro in pending], return_exceptions=True
                )
                for (url, _), result in zip(pending, results):
                    if isinstance(result, Exception):
                        await retry_queue.put(url)
                    elif result is None:
                        await retry_queue.put(url)
                    elif result == "skipped":
                        seen_urls.add(url)
                    else:
                        rows, timings, similar_urls = result
                        timings["worker"] = name
                        await row_queue.put((rows, timings))
                        stats["parsed"] = stats.get("parsed", 0) + 1
                        for sim_url in similar_urls:
                            sim_id = _cid(sim_url)
                            if sim_id and sim_url not in skip_urls_set and sim_id not in seen_urls:
                                seen_urls.add(sim_id)
                                url_queue.put_nowait(sim_url)
                                stats["similar_found"] = stats.get("similar_found", 0) + 1

            cycle_ms = (time.monotonic() - t_cycle) * 1000
            stats["cycle_ms_total"] = stats.get("cycle_ms_total", 0) + cycle_ms
            stats["cycle_count"] = stats.get("cycle_count", 0) + 1
    finally:
        # shared sessions закрываются в run_http_workers
        if shared_sessions is None:
            for s in sessions.values():
                await s.close()


async def run_http_workers(
    n, url_queue, retry_queue, row_queue, pool, stats, cfg,
    cookies=None, seen=None,
):
    refresher = asyncio.create_task(run_refresher(pool, cfg))

    # одна сессия на прокси, шарим между воркерами
    shared_sessions = {}

    tasks = [
        asyncio.create_task(
            http_offer_worker(
                f"H{i+1}",
                url_queue,
                retry_queue,
                row_queue,
                pool,
                stats,
                cfg,
                cookies=cookies,
                seen=seen,
                shared_sessions=shared_sessions,
            )
        )
        for i in range(n)
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        refresher.cancel()
        for s in shared_sessions.values():
            await s.close()


async def fetch_listing(session, url, slot, pool, stats, cfg):
    try:
        resp = await session.get(
            url,
            headers=headers(),
            timeout=cfg.get("http_timeout", 15),
            allow_redirects=True,
        )
    except Exception as e:
        log.debug(f"[LHTTP] {slot.label} network error: {e}")
        return None

    html = resp.text
    if len(html) > 5_000_000:
        return None

    if is_waf(html, resp.status_code):
        pool.report_waf(slot, cfg.get("http_waf_cooldown", 30))
        stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
        return None

    if is_captcha(html, str(resp.url)):
        pool.report_waf(slot, cfg.get("http_captcha_cooldown", 60))
        return None

    if resp.status_code != 200:
        return None

    pool.report_ok(slot)

    soup = BeautifulSoup(html, "lxml")
    cards = soup.find_all("article", attrs={"data-name": "CardComponent"})
    if not cards:
        return []

    skip_urls = cfg.get("skip_url_parts", [])
    skip_phrases = cfg.get("skip_phrases", [])
    results = []
    for card in cards:
        link_area = card.find("div", attrs={"data-name": "LinkArea"})
        if not link_area:
            continue
        a = link_area.find("a", href=True)
        if not a:
            continue
        href = a["href"].split("?")[0]
        if any(part in href for part in skip_urls):
            stats["listing_skip_url"] = stats.get("listing_skip_url", 0) + 1
            continue
        text = card.get_text().lower()
        if any(phrase in text for phrase in skip_phrases):
            stats["listing_skip_phrase"] = stats.get("listing_skip_phrase", 0) + 1
            continue

        # цена из текста
        price = None
        price_match = re.search(r"([\d\s\xa0]{5,})\s*₽", card.get_text())
        if price_match:
            digits = re.sub(r"[^\d]", "", price_match.group(1))
            if digits:
                price = int(digits)

        results.append((href, price))

    stats["listing_cards_total"] = stats.get("listing_cards_total", 0) + len(cards)
    return results


def parse_listing_urls(html, cfg=None):
    soup = BeautifulSoup(html, "lxml")
    cards = soup.find_all("article", attrs={"data-name": "CardComponent"})
    if not cards:
        return []
    skip_urls = (cfg or {}).get("skip_url_parts", [])
    skip_phrases = (cfg or {}).get("skip_phrases", [])
    urls = []
    for card in cards:
        link_area = card.find("div", attrs={"data-name": "LinkArea"})
        if not link_area:
            continue
        a = link_area.find("a", href=True)
        if not a:
            continue
        href = a["href"].split("?")[0]
        if any(part in href for part in skip_urls):
            continue
        text = card.get_text().lower()
        if any(phrase in text for phrase in skip_phrases):
            continue
        urls.append(href)
    return urls


async def http_listing_worker(
    name, filters, url_queue, seen, completed, pool, stats, cfg,
    listing_cache=None, shared_sessions=None,
):
    sessions = shared_sessions if shared_sessions is not None else {}

    async def get_session(proxy):
        if proxy not in sessions:
            sessions[proxy] = AsyncSession(
                impersonate="chrome", proxy=proxy, max_clients=50
            )
        return sessions[proxy]

    max_pages = cfg.get("max_pages", 54)
    max_cached = cfg.get("max_cached_pages", 3)
    min_new = cfg.get("min_new_first_page", 5)

    try:
        for fi, filt in enumerate(filters):
            label = filt["label"]
            log.debug(f"[{name}] FILTER {fi+1}/{len(filters)}: {label}")
            consecutive_cached = 0
            pg = 1

            while pg <= max_pages:
                slot = await pool.acquire()
                if not slot:
                    await asyncio.sleep(0.5)
                    continue

                url = f"{filt['url']}&p={pg}"
                session = await get_session(slot.proxy)
                result = await fetch_listing(session, url, slot, pool, stats, cfg)

                if result is None:
                    # WAF, ретраим с другим слотом
                    await asyncio.sleep(2)
                    continue

                if len(result) == 0:
                    break

                new_count = 0
                cached = 0
                touched = 0
                for href, card_price in result:
                    cid = _cid(href)
                    if not cid:
                        continue

                    if cid in seen:
                        # уже знаем это объявление, помечаем как активное
                        if listing_cache and cid in listing_cache:
                            listing_cache[cid]["_touched"] = True
                        cached += 1
                        stats["listing_cached"] = stats.get("listing_cached", 0) + 1
                        continue
                    seen.add(cid)

                    # новый cid, проверяем нужен ли полный fetch
                    if listing_cache and card_price and cid in listing_cache:
                        old_price = listing_cache[cid].get("price")
                        if old_price and old_price == card_price:
                            # та же цена, достаточно touch
                            listing_cache[cid]["_touched"] = True
                            touched += 1
                            stats["touched"] = stats.get("touched", 0) + 1
                            continue

                    # новое объявление или цена изменилась
                    await url_queue.put(href)
                    new_count += 1

                log.debug(
                    f"[{name}] {label} p.{pg}: cards={len(result)} new={new_count} "
                    f"cached={cached} touched={touched} | queue={url_queue.qsize()}"
                )

                if pg == 1 and new_count < min_new and new_count + cached + touched < 20:
                    break

                # продолжаем только если нашлись реально новые URL-ы
                # touch полезен, но не повод прокручивать все 54 страницы
                if new_count == 0:
                    consecutive_cached += 1
                    if consecutive_cached >= max_cached:
                        break
                else:
                    consecutive_cached = 0

                pg += 1

            completed.append(label)
            stats["filters_done"] = stats.get("filters_done", 0) + 1
            log.debug(f"[{name}] DONE: {label}")
    finally:
        if shared_sessions is None:
            for s in sessions.values():
                await s.close()


async def run_http_listings(
    n, filters, url_queue, seen, completed, pool, stats, cfg, listing_cache=None,
):
    # шарим сессии между listing воркерами
    shared_sessions = {}

    chunks = [filters[i::n] for i in range(n)]
    tasks = [
        asyncio.create_task(
            http_listing_worker(
                f"HL{i+1}", chunks[i], url_queue, seen, completed, pool, stats, cfg,
                listing_cache=listing_cache,
                shared_sessions=shared_sessions,
            )
        )
        for i in range(n)
        if chunks[i]
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for s in shared_sessions.values():
            await s.close()
