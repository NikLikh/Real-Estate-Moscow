import asyncio
import logging
import math
import time

from curl_cffi.requests import AsyncSession

from pipeline.cian.api import api_headers, build_jk_query, build_json_query, parse_search
from pipeline.cian.parsers import extract_cian_id, extract_region_id, map_offer_from_api, parse_offer_from_json, parse_similar_urls_from_html
from pipeline.cian.runtime import should_stop
from pipeline.cian.proxy_farm.detector import is_waf, is_captcha, is_vpn_block, headers
from pipeline.cian.proxy_farm.refresher import run_refresher

log = logging.getLogger("re")


async def _emit_api_offer(offer_obj, cid, href, row_queue, stats, worker):
    try:
        data, price_history = map_offer_from_api(offer_obj)
    except Exception:
        stats["api_errors"] = stats.get("api_errors", 0) + 1
        return False
    if not data:
        return False
    data["url"] = href.split("?")[0]
    data["cian_id"] = cid
    data["_html_price_history"] = price_history
    await row_queue.put(([data], {"worker": worker}))
    stats["api_parsed"] = stats.get("api_parsed", 0) + 1
    stats["parsed"] = stats.get("parsed", 0) + 1
    return True


async def _register_card(cid, href, card_price, offer_obj, seen, listing_cache,
                         url_queue, row_queue, stats, cfg, worker):
    api_extract = bool(cfg.get("api_full_extract")) and row_queue is not None
    info = listing_cache.get(cid) if listing_cache else None
    if info is not None:
        stats["listing_cached"] = stats.get("listing_cached", 0) + 1
        seen.add(cid)
        if card_price and info.get("price") and card_price != info["price"] and not info.get("_repriced"):
            info["_repriced"] = True
            info["price"] = card_price
            info["_touched"] = True
            info["_card_price"] = card_price
            stats["price_changes"] = stats.get("price_changes", 0) + 1
            if api_extract and offer_obj is not None and await _emit_api_offer(offer_obj, cid, href, row_queue, stats, worker):
                return "repriced"
            await url_queue.put(href)
            return "repriced"
        info["_touched"] = True
        if card_price:
            info["_card_price"] = card_price
        return "known"
    if cid in seen:
        stats["listing_cached"] = stats.get("listing_cached", 0) + 1
        return "dup"
    seen.add(cid)
    if api_extract and offer_obj is not None and await _emit_api_offer(offer_obj, cid, href, row_queue, stats, worker):
        return "new"
    await url_queue.put(href)
    return "new"


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
        pool.report_net_error(slot, cfg.get("net_error_threshold", 3), cfg.get("net_error_cooldown", 20), cfg.get("net_error_quarantine", 5))
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

    def _timed_parse(h):
        t = time.monotonic()
        data, hist = parse_offer_from_json(h)
        ms = (time.monotonic() - t) * 1000
        return (data, hist), ms

    (data, price_history), parse_ms = await loop.run_in_executor(None, _timed_parse, html)
    stats["parse_ms_total"] = stats.get("parse_ms_total", 0) + parse_ms
    stats["parse_count"] = stats.get("parse_count", 0) + 1

    if not data or not data.get("price"):
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
    seen=None, shared_sessions=None,
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
                            sim_id = extract_cian_id(sim_url)
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
    seen=None,
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


async def _search_rows(session, body, slot, pool, stats, cfg, endpoint=None):
    await pool.acquire_concurrent(slot)
    try:
        resp = await session.post(
            endpoint or cfg["api_listing_endpoint"],
            json=body,
            headers=api_headers(headers()),
            timeout=cfg.get("http_timeout", 15),
        )
    except Exception:
        pool.release_concurrent(slot)
        stats["net_errors"] = stats.get("net_errors", 0) + 1
        pool.report_net_error(slot, cfg.get("net_error_threshold", 3), cfg.get("net_error_cooldown", 20), cfg.get("net_error_quarantine", 5))
        return None
    pool.release_concurrent(slot)

    if is_waf(resp.text, resp.status_code):
        pool.report_waf(slot, cfg.get("http_waf_cooldown", 30))
        stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
        return None

    if is_captcha(resp.text, str(resp.url)):
        pool.report_waf(slot, cfg.get("http_captcha_cooldown", 60))
        stats["captchas"] = stats.get("captchas", 0) + 1
        return None

    if resp.status_code != 200:
        stats["bad_status"] = stats.get("bad_status", 0) + 1
        return None

    pool.report_ok(slot)
    try:
        data = resp.json()["data"]
    except Exception:
        return None

    _, rows = parse_search(data)
    stats["listing_cards_total"] = stats.get("listing_cards_total", 0) + len(rows)
    return rows


async def http_listing_worker(
    name, filters, url_queue, seen, completed, pool, stats, cfg,
    listing_cache=None, shared_sessions=None, sem=None, zhk_ids=None,
    row_queue=None,
):
    sessions = shared_sessions if shared_sessions is not None else {}
    sem = sem or asyncio.Semaphore(cfg.get("http_listing_concurrency", 100))
    async def get_session(proxy):
        if proxy not in sessions:
            sessions[proxy] = AsyncSession(
                impersonate="chrome", proxy=proxy, max_clients=50
            )
        return sessions[proxy]

    max_pages = cfg.get("max_pages", 54)
    cards_per_page = cfg.get("cards_per_page", 28)

    async def fetch_page(filt, pg):
        async with sem:
            for _ in range(6):
                slot = await pool.acquire()
                if not slot:
                    await asyncio.sleep(0.5)
                    continue
                session = await get_session(slot.proxy)
                result = await _search_rows(session, build_json_query(filt, cfg, pg), slot, pool, stats, cfg)
                if result is None:
                    await asyncio.sleep(1)
                    continue
                return result
            return []

    try:
        for filt in filters:
            label = filt["label"]
            offer_count = filt.get("offer_count") or 0
            pages_for_filter = math.ceil(offer_count / cards_per_page) if offer_count else max_pages
            pages_for_filter = min(pages_for_filter, max_pages)

            pages = await asyncio.gather(*[fetch_page(filt, pg) for pg in range(1, pages_for_filter + 1)])

            for result in pages:
                for href, card_price, jk, offer_obj in result:
                    if jk and zhk_ids is not None:
                        zhk_ids.add(jk)
                    cid = extract_cian_id(href)
                    if not cid:
                        continue
                    await _register_card(cid, href, card_price, offer_obj, seen, listing_cache,
                                         url_queue, row_queue, stats, cfg, name)

            completed.append(label)
            stats["filters_done"] = stats.get("filters_done", 0) + 1
    finally:
        if shared_sessions is None:
            for s in sessions.values():
                await s.close()


async def run_http_listings(
    n, filters, url_queue, seen, completed, pool, stats, cfg, listing_cache=None, zhk_ids=None,
    row_queue=None,
):
    shared_sessions = {}
    default_conc = pool.slot_count * cfg.get("http_max_concurrent_per_proxy", 2)
    sem = asyncio.Semaphore(cfg.get("http_listing_concurrency", default_conc))

    chunks = [filters[i::n] for i in range(n)]
    tasks = [
        asyncio.create_task(
            http_listing_worker(
                f"HL{i+1}", chunks[i], url_queue, seen, completed, pool, stats, cfg,
                listing_cache=listing_cache,
                shared_sessions=shared_sessions,
                sem=sem,
                zhk_ids=zhk_ids,
                row_queue=row_queue,
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


async def run_http_zhk(jk_ids, url_queue, seen, pool, stats, cfg, row_queue=None, listing_cache=None):
    if not jk_ids:
        return
    stats["zhk_total"] = len(jk_ids)
    stats["zhk_done"] = 0
    stats["zhk_new"] = 0
    stats["zhk_capped"] = 0
    shared_sessions = {}
    sem = asyncio.Semaphore(cfg.get("http_listing_concurrency", 100))
    max_pages = cfg.get("max_pages", 54)
    endpoint = cfg.get("api_zhk_endpoint")

    async def get_session(proxy):
        if proxy not in shared_sessions:
            shared_sessions[proxy] = AsyncSession(
                impersonate="chrome", proxy=proxy, max_clients=50
            )
        return shared_sessions[proxy]

    async def drill(jk_id):
        async with sem:
            empty = 0
            fails = 0
            page = 1
            while page <= max_pages and fails < 6:
                slot = await pool.acquire()
                if not slot:
                    fails += 1
                    await asyncio.sleep(0.5)
                    continue
                session = await get_session(slot.proxy)
                rows = await _search_rows(session, build_jk_query(jk_id, page), slot, pool, stats, cfg, endpoint=endpoint)
                if rows is None:
                    fails += 1
                    await asyncio.sleep(1)
                    continue
                fails = 0
                if not rows:
                    empty += 1
                    if empty >= 2:
                        break
                    page += 1
                    continue
                empty = 0
                for href, card_price, _jk, offer_obj in rows:
                    cid = extract_cian_id(href)
                    if not cid:
                        continue
                    outcome = await _register_card(cid, href, card_price, offer_obj, seen, listing_cache,
                                                   url_queue, row_queue, stats, cfg, "zhk")
                    if outcome == "new":
                        stats["zhk_new"] = stats.get("zhk_new", 0) + 1
                page += 1
            if page > max_pages:
                stats["zhk_capped"] = stats.get("zhk_capped", 0) + 1
            stats["zhk_done"] = stats.get("zhk_done", 0) + 1

    try:
        await asyncio.gather(*[drill(j) for j in jk_ids])
    finally:
        for s in shared_sessions.values():
            await s.close()
