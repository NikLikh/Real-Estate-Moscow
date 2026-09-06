import asyncio
import logging
import math
import time

from curl_cffi.requests import AsyncSession

from pipeline.cian.api import api_headers, build_jk_query, build_json_query, parse_search
from pipeline.cian.parsers import extract_cian_id, extract_region_id, map_offer_from_api, parse_offer_from_json, parse_similar_urls_from_html
from pipeline.cian.runtime import should_stop
from pipeline.cian.proxy_farm.detector import MAX_BODY_BYTES, SCAN_CHARS, is_waf, is_captcha, is_vpn_block, headers

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


async def _register_card(cid, href, card_price, offer_obj, deal, seen, listing_cache,
                         url_queue, row_queue, stats, cfg, worker):
    api_extract = bool(cfg.get("api_full_extract")) and row_queue is not None
    info = listing_cache.get(cid) if listing_cache else None
    if info is not None:
        stats["listing_cached"] = stats.get("listing_cached", 0) + 1
        info["_deal"] = deal
        seen.add(cid)
        if card_price and card_price != info.get("price") and not info.get("_repriced"):
            info["_repriced"] = True
            info["price"] = card_price
            info["_touched"] = True
            info["_card_price"] = card_price
            stats["price_changes"] = stats.get("price_changes", 0) + 1
            if api_extract and offer_obj is not None:
                if not await _emit_api_offer(offer_obj, cid, href, row_queue, stats, worker):
                    stats["no_price"] = stats.get("no_price", 0) + 1
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
    if api_extract and offer_obj is not None:
        if not await _emit_api_offer(offer_obj, cid, href, row_queue, stats, worker):
            stats["no_price"] = stats.get("no_price", 0) + 1
        return "new"
    await url_queue.put(href)
    return "new"


async def fetch_offer(session, url, slot, pool, stats, cfg):
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
        log.debug(f"[HTTP] {slot.label} network error: {e}")
        stats["net_errors"] = stats.get("net_errors", 0) + 1
        pool.report_net_error(slot, cfg.get("net_error_threshold", 3), cfg.get("net_error_cooldown", 20), cfg.get("net_error_quarantine", 8))
        return None
    finally:
        pool.release_concurrent(slot)
    fetch_ms = (time.monotonic() - t_fetch) * 1000
    stats["fetch_ms_total"] = stats.get("fetch_ms_total", 0) + fetch_ms
    stats["fetch_count"] = stats.get("fetch_count", 0) + 1

    if len(resp.content) > MAX_BODY_BYTES:
        stats["html_too_large"] = stats.get("html_too_large", 0) + 1
        return "skipped"
    html = resp.text
    if slot.budget and slot.reqs >= slot.budget:
        pool.report_budget(slot, cfg.get("http_budget_cooldown", 300))

    if is_waf(html, resp.status_code):
        pool.report_waf(slot, cfg.get("http_waf_cooldown", 35))
        stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
        return None

    if is_captcha(html, str(resp.url)):
        pool.report_captcha(slot, cfg.get("http_captcha_cooldown", 2),
                            cfg.get("captcha_streak_limit", 6),
                            cfg.get("captcha_streak_cooldown", 1800))
        stats["captchas"] = stats.get("captchas", 0) + 1
        return None

    if is_vpn_block(html):
        pool.report_waf(slot, 60)
        stats["vpn_blocks"] = stats.get("vpn_blocks", 0) + 1
        return None

    if resp.status_code != 200:
        stats["bad_status"] = stats.get("bad_status", 0) + 1
        if resp.status_code in (404, 410):
            return "skipped"
        return None

    pool.report_ok(slot)

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
    seen_urls = seen if seen is not None else set()
    batch_size = cfg.get("http_offer_batch", 1)

    try:
        while not should_stop():
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

            pending = []
            starved = False
            for url in batch:
                slot = await pool.acquire()
                if not slot:
                    await retry_queue.put(url)
                    starved = True
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
                        cid = extract_cian_id(url)
                        if cid:
                            seen_urls.add(cid)
                    else:
                        rows, timings, similar_urls = result
                        timings["worker"] = name
                        await row_queue.put((rows, timings))
                        stats["parsed"] = stats.get("parsed", 0) + 1
                        for sim_url in similar_urls:
                            sim_id = extract_cian_id(sim_url)
                            if any(part in sim_url for part in skip_urls):
                                continue
                            if sim_id and sim_id not in seen_urls:
                                seen_urls.add(sim_id)
                                url_queue.put_nowait(sim_url)
                                stats["similar_found"] = stats.get("similar_found", 0) + 1

            cycle_ms = (time.monotonic() - t_cycle) * 1000
            stats["cycle_ms_total"] = stats.get("cycle_ms_total", 0) + cycle_ms
            stats["cycle_count"] = stats.get("cycle_count", 0) + 1

            if starved:
                stats["pool_starved"] = stats.get("pool_starved", 0) + 1
                await asyncio.sleep(cfg.get("pool_starved_backoff", 2))
    finally:
        if shared_sessions is None:
            for s in sessions.values():
                await s.close()


async def run_http_workers(
    n, url_queue, retry_queue, row_queue, pool, stats, cfg,
    seen=None,
):
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
    except Exception as e:
        stats["net_errors"] = stats.get("net_errors", 0) + 1
        kinds = stats.setdefault("net_kinds", {})
        kinds[type(e).__name__] = kinds.get(type(e).__name__, 0) + 1
        pool.report_net_error(slot, cfg.get("net_error_threshold", 3), cfg.get("net_error_cooldown", 20), cfg.get("net_error_quarantine", 8))
        return "net", None, None
    finally:
        pool.release_concurrent(slot)

    if len(resp.content) > MAX_BODY_BYTES:
        stats["net_errors"] = stats.get("net_errors", 0) + 1
        pool.report_net_error(slot, cfg.get("net_error_threshold", 3), cfg.get("net_error_cooldown", 20), cfg.get("net_error_quarantine", 8))
        return "net", None, None

    head = resp.content[:SCAN_CHARS].decode("utf-8", "ignore")

    if is_waf(head, resp.status_code):
        pool.report_waf(slot, cfg.get("http_waf_cooldown", 35))
        stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
        return "waf", None, None

    if is_captcha(head, str(resp.url)):
        pool.report_captcha(slot, cfg.get("http_captcha_cooldown", 2),
                            cfg.get("captcha_streak_limit", 6),
                            cfg.get("captcha_streak_cooldown", 1800))
        stats["captchas"] = stats.get("captchas", 0) + 1
        return "captcha", None, None

    if resp.status_code != 200:
        stats["bad_status"] = stats.get("bad_status", 0) + 1
        return "status", None, None

    pool.report_ok(slot)
    try:
        data = resp.json()["data"]
    except Exception:
        return "badjson", None, None

    count, rows = parse_search(data)
    stats["listing_cards_total"] = stats.get("listing_cards_total", 0) + len(rows)
    if not rows:
        stats["empty_pages"] = stats.get("empty_pages", 0) + 1
        return "empty", rows, count
    return "ok", rows, count


def _scan_listing_pages(sizes, cards_per_page):
    exhausted = any(n == 0 for n in sizes)
    lost = sum(1 for n in sizes if n is None)
    return exhausted, lost


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
    page_tries = cfg.get("listing_page_tries", 8)
    page_batch = cfg.get("listing_page_batch", 4)
    empty_tries = cfg.get("listing_empty_tries", 3)
    slot_wait_limit = cfg.get("slot_wait_limit", 120)

    async def fetch_page(filt, pg, expect_full):
        body = build_json_query(filt, cfg, pg)
        async with sem:
            stats["listing_inflight"] = stats.get("listing_inflight", 0) + 1
            try:
                return await _fetch_page_inner(filt, pg, expect_full, body)
            finally:
                stats["listing_inflight"] -= 1

    async def _fetch_page_inner(filt, pg, expect_full, body):
        empty_seen = 0
        tries = 0
        waits = 0
        while tries < page_tries and waits < slot_wait_limit:
            if should_stop():
                return None, None
            slot = await pool.acquire()
            if not slot:
                waits += 1
                await asyncio.sleep(0.5)
                continue
            tries += 1
            session = await get_session(slot.proxy)
            status, rows, count = await _search_rows(session, body, slot, pool, stats, cfg)
            if status == "ok":
                return rows, count
            if status == "empty":
                empty_seen += 1
                if not expect_full or empty_seen >= empty_tries:
                    return rows, count
        return None, None

    try:
        for filt in filters:
            if should_stop():
                break
            label = filt["label"]
            offer_count = filt.get("offer_count") or 0
            bound = min(math.ceil(offer_count / cards_per_page), max_pages) if offer_count else max_pages
            full_pages = min(offer_count // cards_per_page, max_pages) if offer_count else 0

            sizes = []
            pg = 1
            real_count = None
            got = 0
            while pg <= bound:
                tail = next((n for n in reversed(sizes) if n is not None), None)
                partial = tail is not None and 0 < tail < cards_per_page
                step = 1 if real_count is None or partial else page_batch
                last = min(pg + step - 1, bound)
                chunk = await asyncio.gather(*[
                    fetch_page(filt, p, p == 1 if real_count is None else p <= full_pages)
                    for p in range(pg, last + 1)
                ])
                if real_count is None:
                    for _rows, cnt in chunk:
                        if cnt:
                            real_count = cnt
                            stats["count_corrected"] = stats.get("count_corrected", 0) + (cnt != offer_count)
                            bound = min(math.ceil(cnt / cards_per_page), max_pages)
                            full_pages = min(cnt // cards_per_page, max_pages)
                            break
                for rows, _c in chunk:
                    sizes.append(None if rows is None else len(rows))
                    if not rows:
                        continue
                    got += len(rows)
                    for href, card_price, jk, deal, offer_obj in rows:
                        if jk and zhk_ids is not None:
                            zhk_ids.add(jk)
                        cid = extract_cian_id(href)
                        if not cid:
                            continue
                        await _register_card(cid, href, card_price, offer_obj, deal, seen, listing_cache,
                                             url_queue, row_queue, stats, cfg, name)
                if real_count and got >= real_count:
                    break
                edge = next((n for n in reversed(sizes) if n is not None), None)
                if real_count and edge and edge < cards_per_page and got + cards_per_page > real_count:
                    break
                if _scan_listing_pages(sizes, cards_per_page)[0]:
                    break
                pg = last + 1

            failed_pages = sum(1 for n in sizes if n is None)
            if failed_pages:
                stats["pages_failed"] = stats.get("pages_failed", 0) + failed_pages
                stats["filters_incomplete"] = stats.get("filters_incomplete", 0) + 1
            else:
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
    max_pages = cfg.get("zhk_max_pages", 40)
    slot_wait_limit = cfg.get("slot_wait_limit", 120)
    endpoint = cfg.get("api_zhk_endpoint")

    async def get_session(proxy):
        if proxy not in shared_sessions:
            shared_sessions[proxy] = AsyncSession(
                impersonate="chrome", proxy=proxy, max_clients=50
            )
        return shared_sessions[proxy]

    async def drill(jk_id):
        if should_stop():
            return
        async with sem:
            fails = 0
            waits = 0
            page = 1
            while page <= max_pages and fails < 6 and waits < slot_wait_limit and not should_stop():
                slot = await pool.acquire()
                if not slot:
                    waits += 1
                    await asyncio.sleep(0.5)
                    continue
                session = await get_session(slot.proxy)
                body = build_jk_query(jk_id, page)
                status, rows, _c = await _search_rows(session, body, slot, pool, stats, cfg, endpoint=endpoint)
                if rows is None:
                    fails += 1
                    await asyncio.sleep(1)
                    continue
                fails = 0
                if not rows:
                    break
                for href, card_price, _jk, deal, offer_obj in rows:
                    cid = extract_cian_id(href)
                    if not cid:
                        continue
                    outcome = await _register_card(cid, href, card_price, offer_obj, deal, seen, listing_cache,
                                                   url_queue, row_queue, stats, cfg, "zhk")
                    if outcome == "new":
                        stats["zhk_new"] = stats.get("zhk_new", 0) + 1
                page += 1
            if page > max_pages:
                stats["zhk_capped"] = stats.get("zhk_capped", 0) + 1
            stats["zhk_done"] = stats.get("zhk_done", 0) + 1

    tasks = [asyncio.create_task(drill(j)) for j in jk_ids]
    try:
        done, pending = await asyncio.wait(tasks, timeout=cfg.get("zhk_phase_timeout", 420))
        for t in pending:
            t.cancel()
        if pending:
            log.info(f"[PHASE 0] таймаут, отменено {len(pending)} ЖК")
    finally:
        for s in shared_sessions.values():
            await s.close()
