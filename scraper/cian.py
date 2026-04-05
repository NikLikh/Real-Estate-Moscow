"""Скрапер cian.ru"""

import asyncio
import logging
import random
import time
from datetime import datetime

from bs4 import BeautifulSoup
from patchright.async_api import async_playwright

from config.settings import load_scraper_config
from db.repository import get_cached_urls, save_rows
from scraper.browser import (
    AdaptiveDelay,
    detect_captcha,
    detect_vpn_block,
    detect_waf_rate_limit,
    humanize,
    jittered_delay,
    launch_browser_pool,
)
from scraper.http_offers import build_http_pool, run_http_listings, run_http_workers
from scraper.parsers import parse_offer_page
from scraper.planner import build_filters_from_config, http_plan_filters, plan_filters
from scraper.proxy import ensure_vds_tunnel, resolve_runtime_endpoints, stop_vds_tunnel
from scraper.vpn_ext import cleanup_temp_dirs
from scraper.runtime import (
    EndpointEvent,
    EndpointOrchestrator,
    EndpointRegistry,
    EndpointSession,
    build_runtime_session_plan,
    clear_checkpoint,
    install_shutdown_handler,
    is_restarting,
    is_shutting_down,
    load_checkpoint,
    queue_snapshot,
    request_restart,
    reset_restart,
    save_checkpoint,
    save_dead_letter,
    should_stop,
)

log = logging.getLogger("re")


def _format_slot_plan(names, registry):
    if not names:
        return "-"
    parts = []
    for i, name in enumerate(names):
        ep = registry.get(name).endpoint_dict()
        parts.append(f"{i + 1}:{name}[{ep['slot_class']}]")
    return ", ".join(parts)


async def supervised(name, coro_factory, max_crashes):
    # перезапускает воркер если он упал, до max_crashes раз
    crashes = 0
    while crashes < max_crashes and not should_stop():
        try:
            await coro_factory()
            break
        except asyncio.CancelledError:
            break
        except Exception as e:
            crashes += 1
            log.error(f"[{name}] CRASH #{crashes}/{max_crashes}: {e}")
            if crashes >= max_crashes:
                log.error(f"[{name}] max crashes reached")
                break
            await jittered_delay(5.0, 15.0)


async def crawl_listings(
    name,
    browser,
    filters,
    url_queue,
    seen,
    sem,
    completed,
    delay,
    stats,
    cfg,
    orchestrator=None,
    stagger=0,
    endpoint_name=None,
    pw=None,
    shared=False,
):
    # ходит по листингам и собирает url-ы офферов в очередь
    max_pages = cfg.get("max_pages", 54)
    max_cached = cfg.get("max_cached_pages", 3)
    min_new = cfg.get("min_new_first_page", 5)
    n_offer = cfg.get("_runtime_offer_slots") or cfg.get("offer_workers", 8)
    skip_urls = cfg.get("skip_url_parts", [])
    skip_phrases = cfg.get("skip_phrases", [])
    listing_batch = cfg.get("listing_batch_size", 5)
    batch_cooldown = cfg.get("batch_cooldown", [8.0, 15.0])

    if stagger:
        await asyncio.sleep(stagger)

    s = EndpointSession(
        name,
        "listing",
        browser,
        orchestrator,
        prefer_name=endpoint_name,
        pw=pw,
        cfg=cfg,
        shared=shared,
    )
    await s.open()
    consecutive_failed_filters = 0
    consecutive_waf = 0

    try:
        for fi, filt in enumerate(filters):
            if should_stop():
                break

            label = filt["label"]
            log.info(f"\n{'='*50}")
            log.info(f"[{name}] FILTER {fi+1}/{len(filters)}: {label}")
            log.info(f"{'='*50}")
            consecutive_page_fails = 0
            consecutive_cached = 0
            filter_ok = False

            for pg in range(1, max_pages + 1):
                if should_stop():
                    break
                if consecutive_page_fails >= 3:
                    log.info(f"[{name}] 3+ fails on {label}, skip filter")
                    break
                if consecutive_cached >= max_cached:
                    log.info(f"[{name}] {max_cached} pages cached, skip filter")
                    filter_ok = True
                    break

                await s.maybe_restart_batch(listing_batch, batch_cooldown)

                # backpressure
                if not cfg.get("_serial_offer_phase"):
                    threshold = 5 * n_offer
                    while url_queue.qsize() > threshold and not should_stop():
                        await asyncio.sleep(2)

                if delay.under_pressure:
                    await jittered_delay(3.0, 6.0)

                url = f"{filt['url']}&p={pg}"
                log.info(f"[{name}] {label} p.{pg}/{max_pages}")

                result, _ = await s.goto(url, sem)
                if result == "network":
                    await s.rotate(EndpointEvent.NETWORK, "network error")
                    consecutive_page_fails += 1
                    await jittered_delay(3.0, 6.0)
                    continue
                if not result:
                    consecutive_page_fails += 1
                    continue

                if await detect_waf_rate_limit(s.page):
                    stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
                    recovered = False
                    if await detect_captcha(s.page, url_only=True):
                        stats["captchas"] += 1
                        delay.captcha_enter()
                        try:
                            ok = await s.handle_captcha(url)
                        finally:
                            delay.captcha_exit()
                        if ok:
                            result, _ = await s.goto(url, sem)
                            recovered = (
                                result
                                and not await detect_waf_rate_limit(s.page)
                                and not await detect_captcha(s.page, url_only=True)
                            )
                    if recovered:
                        consecutive_waf = 0
                        await s.report_waf_resolved()
                    else:
                        consecutive_waf += 1
                        log.warning(
                            f"[{name}] WAF #{consecutive_waf} on {s.ep['name']}"
                        )
                        if consecutive_waf >= 6:
                            request_restart(f"WAF x{consecutive_waf}")
                            break
                        await s.rotate(EndpointEvent.WAF, "waf")
                        await jittered_delay(2.0, 5.0)
                        continue

                consecutive_waf = 0
                await s.report_success()

                if await detect_captcha(s.page, url_only=True):
                    delay.captcha_enter()
                    try:
                        ok = await s.handle_captcha(url)
                    finally:
                        delay.captcha_exit()
                    if ok:
                        result, _ = await s.goto(url, sem)
                        if not result or await detect_captcha(s.page, url_only=True):
                            consecutive_page_fails += 1
                            continue
                    else:
                        consecutive_page_fails += 1
                        await s.rotate(EndpointEvent.CAPTCHA, "captcha failed")
                        await delay.wait()
                        continue

                if await detect_vpn_block(s.page):
                    await s.rotate(EndpointEvent.NETWORK, "VPN block")
                    consecutive_page_fails += 1
                    await jittered_delay(3.0, 6.0)
                    continue

                try:
                    await s.page.wait_for_selector(
                        'article[data-name="CardComponent"]', timeout=5000
                    )
                except Exception:
                    if await detect_captcha(s.page, url_only=True):
                        consecutive_page_fails += 1
                        continue
                    try:
                        title = await s.page.title()
                        log.info(
                            f"[{name}] no cards on p.{pg}, title='{title[:60]}', end of filter"
                        )
                    except Exception:
                        log.info(f"[{name}] no cards on p.{pg}, end of filter")
                    filter_ok = True
                    break

                await humanize(s.page)

                cards = await s.page.query_selector_all(
                    'article[data-name="CardComponent"]'
                )
                if not cards:
                    log.info(f"[{name}] no cards, stop filter")
                    filter_ok = True
                    break

                new_count = 0
                cached = 0
                for card in cards:
                    link = await card.query_selector('div[data-name="LinkArea"] a')
                    if not link:
                        continue
                    href = await link.get_attribute("href")
                    if not href:
                        continue
                    href = href.split("?")[0]

                    if href in seen:
                        cached += 1
                        continue
                    if any(part in href for part in skip_urls):
                        continue

                    card_text = (await card.inner_text()).lower()
                    if any(phrase in card_text for phrase in skip_phrases):
                        continue

                    seen.add(href)
                    await url_queue.put(href)
                    new_count += 1

                log.info(
                    f"[{name}] p.{pg}: cards={len(cards)} new={new_count} cached={cached} "
                    f"| queue={url_queue.qsize()}"
                )

                if pg == 1 and new_count < min_new and new_count + cached < 20:
                    log.info(f"[{name}] sparse filter ({new_count} new), skip")
                    filter_ok = True
                    break

                if new_count == 0:
                    consecutive_cached += 1
                else:
                    consecutive_cached = 0

                filter_ok = True
                consecutive_page_fails = 0
                delay.report_success()
                await jittered_delay(0.8, 1.5)

            if should_stop():
                break

            if filter_ok:
                consecutive_failed_filters = 0
                completed.append(label)
                log.info(f"[{name}] DONE: {label} ({len(completed)} filters total)")
            else:
                consecutive_failed_filters += 1
                log.warning(
                    f"[{name}] FAILED: {label} "
                    f"({consecutive_failed_filters} in a row)"
                )
                if consecutive_failed_filters >= 5:
                    request_restart("5 filters failed in a row")
                    break
    finally:
        await s.close()


async def _parse_and_save(
    name,
    page,
    url,
    row_queue,
    stats,
    t_start=None,
    goto_dt=0,
    check_dt=0,
    max_retries=5,
):
    for attempt in range(max_retries):
        try:
            await page.wait_for_selector('[data-testid="price-amount"]', timeout=4000)
        except Exception:
            await asyncio.sleep(0.3)

        try:
            html = await page.content()
        except Exception:
            stats["skipped"] += 1
            return False

        soup = BeautifulSoup(html, "html.parser")
        title_el = soup.find(attrs={"data-name": "OfferTitleNew"})
        if title_el:
            t = title_el.get_text(strip=True).lower()
            if "комната" in t or "доля" in t:
                stats["skipped"] += 1
                return True

        data, price_history = parse_offer_page(html)

        if data.get("price"):
            break

        # если цены нет, скорее всего rate limit или битая страница
        if attempt < max_retries - 1:
            log.debug(f"[{name}] no price, reload ({attempt + 1}/{max_retries})")
            await jittered_delay(2.0, 5.0)
            try:
                await page.reload(timeout=30000, wait_until="domcontentloaded")
            except Exception:
                pass

    parse_dt = 0  # timing неточный после retries, но не критично

    data["url"] = url.split("?")[0]
    data["source"] = "cian"
    data["parsed_at"] = datetime.now().isoformat()

    # каждое изменение цены сохраняем отдельной строкой
    rows = [data]
    cur_price = data.get("price")
    for entry in price_history:
        if entry["price"] == cur_price:
            continue
        hist = data.copy()
        hist["price"] = entry["price"]
        hist["publication_date"] = entry["date"]
        hist["source"] = "cian_history"
        rows.append(hist)

    # тайминги нужны чтобы видеть что тормозит при деградации
    timings = {
        "worker": name,
        "t_start": t_start,
        "goto_dt": goto_dt,
        "check_dt": check_dt,
        "parse_dt": parse_dt,
    }
    await row_queue.put((rows, timings))
    stats["parsed"] += 1
    return True


async def parse_offers(
    name,
    browser,
    url_queue,
    retry_queue,
    row_queue,
    sem,
    delay,
    stats,
    cfg,
    orchestrator=None,
    stagger=0,
    endpoint_name=None,
    pw=None,
):
    if stagger:
        await asyncio.sleep(stagger)

    batch_size = cfg.get("batch_size", 25)
    batch_cooldown = cfg.get("batch_cooldown", [2.0, 4.0])
    s = EndpointSession(
        name, "offer", browser, orchestrator, prefer_name=endpoint_name, pw=pw, cfg=cfg
    )
    await s.open()
    consecutive_waf = 0

    try:
        while True:
            try:
                url = await asyncio.wait_for(url_queue.get(), timeout=60)
            except asyncio.TimeoutError:
                if should_stop():
                    break
                continue

            if url is None:
                break
            if should_stop():
                break

            await s.maybe_restart_batch(batch_size, batch_cooldown)

            if delay.under_pressure:
                await jittered_delay(3.0, 6.0)

            t_start = time.monotonic()

            result, goto_dt = await s.goto(url, sem)
            if result == "network":
                stats["network_errors"] = stats.get("network_errors", 0) + 1
                await s.rotate(EndpointEvent.NETWORK, "network error")
                await retry_queue.put(url)
                await jittered_delay(2.0, 4.0)
                continue
            if not result:
                await retry_queue.put(url)
                await jittered_delay(0.5, 1.0)
                continue

            if await detect_waf_rate_limit(s.page):
                stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
                recovered = False
                if await detect_captcha(s.page, url_only=True):
                    stats["captchas"] += 1
                    delay.captcha_enter()
                    try:
                        ok = await s.handle_captcha(url)
                    finally:
                        delay.captcha_exit()
                    if ok:
                        result, goto_dt = await s.goto(url, sem)
                        recovered = (
                            result
                            and not await detect_waf_rate_limit(s.page)
                            and not await detect_captcha(s.page, url_only=True)
                        )
                if recovered:
                    consecutive_waf = 0
                    await s.report_waf_resolved()
                else:
                    consecutive_waf += 1
                    log.warning(f"[{name}] WAF #{consecutive_waf} on {s.ep['name']}")
                    await url_queue.put(url)
                    if consecutive_waf >= 6:
                        request_restart(f"WAF x{consecutive_waf}")
                        break
                    await s.rotate(EndpointEvent.WAF, "waf")
                    await jittered_delay(2.0, 5.0)
                    continue

            consecutive_waf = 0
            await s.report_success()

            t_check = time.monotonic()
            if await detect_captcha(s.page, url_only=True):
                stats["captchas"] += 1
                delay.captcha_enter()
                try:
                    ok = await s.handle_captcha(url)
                finally:
                    delay.captcha_exit()
                if ok:
                    result, goto_dt = await s.goto(url, sem)
                    if not result or await detect_captcha(s.page, url_only=True):
                        await retry_queue.put(url)
                        continue
                else:
                    await s.rotate(EndpointEvent.CAPTCHA, "captcha failed")
                    await retry_queue.put(url)
                    await jittered_delay(2.0, 4.0)
                    continue
            check_dt = time.monotonic() - t_check

            # имитация чтения страницы, WAF может трекать dwell time
            if random.random() < 0.3:
                await humanize(s.page)

            await _parse_and_save(
                name, s.page, url, row_queue, stats, t_start, goto_dt, check_dt
            )
            delay.report_success()

            p = stats["parsed"]
            waf = stats.get("waf_blocks", 0)
            if p > 0 and p % 10 == 0:
                log.info(
                    f"[{name}] parsed={p} skip={stats['skipped']} "
                    f"cap={stats['captchas']} waf={waf} ep={s.ep['name']} "
                    f"| queue={url_queue.qsize()}"
                )

            await jittered_delay(0.3, 0.8)
    finally:
        await s.close()


async def retry_offers(
    name,
    browser,
    retry_queue,
    url_queue,
    row_queue,
    sem,
    delay,
    stats,
    orchestrator=None,
    stagger=0,
    cfg=None,
    endpoint_name=None,
    pw=None,
):
    if stagger:
        await asyncio.sleep(stagger)

    cfg = cfg or {}
    batch_size = cfg.get("batch_size", 25)
    batch_cooldown = cfg.get("batch_cooldown", [2.0, 4.0])

    s = EndpointSession(
        name, "retry", browser, orchestrator, prefer_name=endpoint_name, pw=pw, cfg=cfg
    )
    await s.open()
    consecutive_fails = 0
    consecutive_waf = 0
    url_fail_counts = {}

    try:
        while True:
            try:
                url = await asyncio.wait_for(retry_queue.get(), timeout=90)
            except asyncio.TimeoutError:
                if should_stop():
                    break
                continue

            if url is None:
                break
            if should_stop():
                break

            url_fail_counts[url] = url_fail_counts.get(url, 0) + 1
            if url_fail_counts[url] > 3:
                save_dead_letter("cian", url, "max retries")
                stats["skipped"] += 1
                continue

            await s.maybe_restart_batch(batch_size, batch_cooldown)
            await jittered_delay(2.0, 5.0)

            t_start = time.monotonic()

            result, goto_dt = await s.goto(url, sem)
            if result == "network":
                stats["network_errors"] = stats.get("network_errors", 0) + 1
                await s.rotate(EndpointEvent.NETWORK, "network error")
                consecutive_fails = 0
                await jittered_delay(3.0, 6.0)
                continue
            if not result:
                consecutive_fails += 1
                stats["skipped"] += 1
                if consecutive_fails >= 5:
                    await s.rotate(EndpointEvent.NETWORK, f"{consecutive_fails} fails")
                    consecutive_fails = 0
                    await jittered_delay(5.0, 10.0)
                continue

            if await detect_waf_rate_limit(s.page):
                stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
                recovered = False
                if await detect_captcha(s.page, url_only=True):
                    stats["captchas"] += 1
                    delay.captcha_enter()
                    try:
                        ok = await s.handle_captcha(url)
                    finally:
                        delay.captcha_exit()
                    if ok:
                        result, goto_dt = await s.goto(url, sem)
                        recovered = (
                            result
                            and not await detect_waf_rate_limit(s.page)
                            and not await detect_captcha(s.page, url_only=True)
                        )
                if recovered:
                    consecutive_waf = 0
                    await s.report_waf_resolved()
                else:
                    consecutive_waf += 1
                    log.warning(f"[{name}] WAF #{consecutive_waf} on {s.ep['name']}")
                    await url_queue.put(url)
                    if consecutive_waf >= 6:
                        request_restart(f"WAF x{consecutive_waf}")
                        break
                    await s.rotate(EndpointEvent.WAF, "waf")
                    await jittered_delay(2.0, 5.0)
                    continue

            consecutive_waf = 0
            await s.report_success()

            if await detect_captcha(s.page, url_only=True):
                stats["captchas"] += 1
                delay.captcha_enter()
                try:
                    ok = await s.handle_captcha(url)
                finally:
                    delay.captcha_exit()
                if not ok:
                    await s.rotate(EndpointEvent.CAPTCHA, "captcha failed")
                    await jittered_delay(2.0, 4.0)
                    continue
                result, goto_dt = await s.goto(url, sem)
                if not result or await detect_captcha(s.page, url_only=True):
                    continue

            if await _parse_and_save(
                name, s.page, url, row_queue, stats, t_start, goto_dt
            ):
                consecutive_fails = 0
                del url_fail_counts[url]
                delay.report_success()

            p = stats["parsed"]
            waf = stats.get("waf_blocks", 0)
            if p > 0 and p % 10 == 0:
                log.info(
                    f"[{name}] parsed={p} skip={stats['skipped']} "
                    f"cap={stats['captchas']} waf={waf} ep={s.ep['name']}"
                )
    finally:
        await s.close()


async def flush_rows(row_queue, stats):
    # отдельный воркер для записи в БД, чтобы не блокировать парсеры
    while True:
        try:
            item = await asyncio.wait_for(row_queue.get(), timeout=30)
        except asyncio.TimeoutError:
            if should_stop():
                break
            continue

        if item is None:
            break

        rows, timings = item
        current = sum(1 for r in rows if r.get("source") == "cian")
        history = sum(1 for r in rows if r.get("source") == "cian_history")

        # если цены нет вообще, значит страница не загрузилась нормально
        has_price = sum(1 for r in rows if r.get("price"))
        if not has_price:
            worker = timings.get("worker", "?")
            url = rows[0].get("url", "?") if rows else "?"
            log.warning(f"[DB:{worker}] SKIP no price | url={url[-40:]}")
            stats["saved"] += 0
            continue

        t_db = time.monotonic()
        saved = save_rows(rows)
        db_dt = time.monotonic() - t_db

        stats["saved"] += saved
        if not saved:
            worker = timings.get("worker", "?")
            url = rows[0].get("url", "?") if rows else "?"
            log.debug(f"[DB:{worker}] 0 saved (duplicate?) | url={url[-40:]}")

        if saved:
            worker = timings.get("worker", "?")
            parts = [f"[DB:{worker}] +{saved} ({current} offers, {history} history)"]

            t_start = timings.get("t_start")
            if t_start:
                total = time.monotonic() - t_start
                goto = timings.get("goto_dt", 0)
                check = timings.get("check_dt", 0)
                parse = timings.get("parse_dt", 0)
                # goto=сеть, check=vpn+капча, parse=HTML, db=запись
                parts.append(
                    f"total {total:.1f}s "
                    f"(goto {goto:.1f} + check {check:.1f} + parse {parse:.1f} + db {db_dt:.1f})"
                )

            log.info(" | ".join(parts))


async def memory_watchdog(threshold_mb):
    # chromium течет, рестартим браузер если RSS вылез за лимит
    import psutil

    proc = psutil.Process()
    while not should_stop():
        rss = proc.memory_info().rss / 1024 / 1024
        if rss > threshold_mb:
            log.warning(f"[MEM] {rss:.0f}MB > {threshold_mb}MB, requesting restart")
            request_restart(f"memory {rss:.0f}MB")
            break
        await asyncio.sleep(30)


def _build_runtime_checkpoint(
    completed, url_queue, retry_queue, session_plan, registry
):
    return {
        "completed_filters": list(completed),
        "pending_urls": queue_snapshot(url_queue),
        "retry_urls": queue_snapshot(retry_queue),
        "runtime_session_plan": session_plan.to_dict() if session_plan else None,
        "endpoint_snapshots": registry.snapshots() if registry else [],
    }


async def _restore_runtime_queues(checkpoint, url_queue, retry_queue, seen):
    pending = checkpoint.get("pending_urls", []) if checkpoint else []
    retry = checkpoint.get("retry_urls", []) if checkpoint else []

    for url in pending:
        seen.add(url)
        await url_queue.put(url)

    for url in retry:
        seen.add(url)
        await retry_queue.put(url)

    return len(pending), len(retry)


async def checkpoint_runtime_periodically(
    completed, url_queue, retry_queue, session_plan, registry, interval=30
):
    while not should_stop():
        save_checkpoint(
            "cian",
            _build_runtime_checkpoint(
                completed, url_queue, retry_queue, session_plan, registry
            ),
        )
        await asyncio.sleep(interval)


async def print_stats_periodically(stats, t0, registry=None, interval=300):
    while not should_stop():
        await asyncio.sleep(interval)
        elapsed_min = (time.monotonic() - t0) / 60 if t0 else 1
        rate = stats["parsed"] / elapsed_min if elapsed_min > 0 else 0
        net = stats.get("network_errors", 0)
        waf = stats.get("waf_blocks", 0)
        log.info(
            f"\n{'-'*50}\n"
            f"[STATS] {elapsed_min:.0f}min elapsed\n"
            f"  parsed={stats['parsed']} saved={stats['saved']}\n"
            f"  captchas={stats['captchas']} waf_blocks={waf}\n"
            f"  skipped={stats['skipped']} net_errors={net}\n"
            f"  rate: {rate:.1f} offers/min\n"
            f"{'-'*50}"
        )
        if registry:
            for line in registry.format_lines():
                log.info(f"[EP] {line}")


async def _run_serial_offer_retry(
    browser_pool,
    endpoint_name,
    pw,
    url_queue,
    retry_queue,
    row_queue,
    sem,
    delay,
    stats,
    cfg,
    orchestrator,
    max_crashes,
):
    while queue_snapshot(url_queue) or queue_snapshot(retry_queue):
        if queue_snapshot(url_queue):
            await url_queue.put(None)
            await supervised(
                "P1",
                lambda b=browser_pool.get(), pw=pw: parse_offers(
                    "P1",
                    b,
                    url_queue,
                    retry_queue,
                    row_queue,
                    sem,
                    delay,
                    stats,
                    cfg,
                    orchestrator=orchestrator,
                    endpoint_name=endpoint_name,
                    pw=pw,
                ),
                max_crashes,
            )

        if queue_snapshot(retry_queue):
            await retry_queue.put(None)
            await supervised(
                "R1",
                lambda b=browser_pool.get(), pw=pw: retry_offers(
                    "R1",
                    b,
                    retry_queue,
                    url_queue,
                    row_queue,
                    sem,
                    delay,
                    stats,
                    orchestrator=orchestrator,
                    cfg=cfg,
                    endpoint_name=endpoint_name,
                    pw=pw,
                ),
                max_crashes,
            )


async def _run_session(all_filters, seen, stats, cfg, t0):
    checkpoint = load_checkpoint("cian") or {}
    done_labels = set(checkpoint.get("completed_filters", []))
    if done_labels:
        log.info(f"checkpoint: {len(done_labels)} filters done")

    remaining = [f for f in all_filters if f["label"] not in done_labels]
    completed = list(done_labels)
    restored_pending = checkpoint.get("pending_urls", [])
    restored_retry = checkpoint.get("retry_urls", [])
    log.info(f"remaining: {len(remaining)} of {len(all_filters)}")

    if not remaining and not restored_pending and not restored_retry:
        log.info("all filters done!")
        clear_checkpoint("cian")
        return completed

    registry = EndpointRegistry(cfg.get("verified_endpoints") or [])
    if checkpoint.get("endpoint_snapshots"):
        registry.restore(checkpoint["endpoint_snapshots"])
    registry.refresh()
    if not registry.healthy_endpoints():
        for name in registry.names():
            state = registry.get(name)
            if state.lifecycle.value == "dead":
                continue
            registry.mark_healthy(name, "restart rewarm", state.network_id)
    orchestrator = EndpointOrchestrator(registry, cfg)

    http_pool = await build_http_pool(cfg)
    n_http = cfg.get("http_offer_workers", 20)
    use_http = http_pool.alive > 0
    if use_http:
        log.info(f"[HTTP] pool ready: {http_pool.alive} slots, {n_http} workers")

    session_plan = build_runtime_session_plan(
        cfg,
        len(remaining),
        registry.snapshots(),
        http_offers=use_http,
    )
    planner_workers = session_plan.planner_workers
    max_concurrent = session_plan.max_concurrent
    if use_http:
        queue_max = 5000  # httpx workers drain fast, но не unbounded на случай если все slots cooling
    elif session_plan.serial_offer_phase:
        queue_max = 0
    else:
        queue_max = max(
            cfg.get("url_queue_max", 300),
            len(restored_pending) + len(restored_retry) + 50,
        )
    max_crashes = cfg.get("max_worker_crashes", 5)
    mem_threshold = cfg.get("memory_threshold_mb", 2500)
    listing_slots = session_plan.listing_slots
    offer_slots = session_plan.offer_slots
    retry_slots = session_plan.retry_slots
    total_offer = session_plan.total_offer
    total_retry = session_plan.total_retry
    n_browsers = session_plan.n_browsers
    cfg["_runtime_offer_slots"] = max(1, total_offer)
    cfg["_serial_offer_phase"] = session_plan.serial_offer_phase

    sem = asyncio.Semaphore(max_concurrent)
    url_queue = asyncio.Queue(maxsize=queue_max)
    retry_queue = asyncio.Queue()
    row_queue = asyncio.Queue()
    delay = AdaptiveDelay()
    restored_counts = await _restore_runtime_queues(
        checkpoint, url_queue, retry_queue, seen
    )

    runtime_desc = ", ".join(
        f"{ep['name']}[{ep['slot_class']}:{ep.get('network_id', ep.get('ip', '-'))}]"
        for ep in registry.runtime_endpoints()
    )
    log.info(f"[PLAN] runtime endpoints: {runtime_desc}")
    log.info(
        f"[PLAN] sessions: {len(listing_slots)} listing + {total_offer} offer "
        f"+ {total_retry} retry, sem={max_concurrent}, browsers={n_browsers}, "
        f"planner={planner_workers}, serial={session_plan.serial_offer_phase}"
    )
    log.info(f"[PLAN] listing slots: {_format_slot_plan(listing_slots, registry)}")
    log.info(f"[PLAN] offer slots: {_format_slot_plan(offer_slots, registry)}")
    log.info(f"[PLAN] retry slots: {_format_slot_plan(retry_slots, registry)}")
    if restored_counts != (0, 0):
        log.info(
            f"[PLAN] restored queues: pending={restored_counts[0]} retry={restored_counts[1]}"
        )

    async with async_playwright() as pw:
        browser_pool = await launch_browser_pool(
            pw, n_browsers, headless=cfg.get("headless", True)
        )

        # curl_cffi имитирует Chrome TLS, cookies не нужны
        http_cookies = None

        checkpoint_task = None
        writer_task = None
        watchdog_task = None
        stats_task = None
        try:
            filters = []
            if remaining:
                if use_http:
                    filters = await http_plan_filters(http_pool, cfg)
                else:
                    filters = await plan_filters(
                        browser_pool, sem, cfg, orchestrator=orchestrator, pw=pw
                    )
                filters = [f for f in filters if f["label"] not in done_labels]
                log.info(f"after plan: {len(filters)} filters to crawl")

            writer_task = asyncio.create_task(flush_rows(row_queue, stats))
            watchdog_task = asyncio.create_task(memory_watchdog(mem_threshold))
            stats_task = asyncio.create_task(
                print_stats_periodically(stats, t0, registry=registry)
            )
            checkpoint_task = asyncio.create_task(
                checkpoint_runtime_periodically(
                    completed,
                    url_queue,
                    retry_queue,
                    session_plan,
                    registry,
                )
            )

            listing_tasks = []
            if use_http and filters:
                # listing тоже через curl_cffi -- все 60 IP для listing + offers
                n_listing = cfg.get("http_listing_workers", 8)
                http_task = asyncio.create_task(
                    run_http_workers(
                        n_http,
                        url_queue,
                        retry_queue,
                        row_queue,
                        http_pool,
                        stats,
                        cfg,
                        cookies=http_cookies,
                    )
                )
                await run_http_listings(
                    n_listing,
                    filters,
                    url_queue,
                    seen,
                    completed,
                    http_pool,
                    stats,
                    cfg,
                )
                for _ in range(n_http):
                    await url_queue.put(None)
                await http_task

            elif not use_http and filters and listing_slots:
                filter_chunks = [
                    filters[i :: len(listing_slots)] for i in range(len(listing_slots))
                ]
                listing_stagger = cfg.get("listing_stagger", 3.0)
                listing_tasks = [
                    asyncio.create_task(
                        supervised(
                            f"L{i+1}",
                            lambda i=i, b=browser_pool.get(), chunk=filter_chunks[
                                i
                            ], endpoint_name=listing_slots[i], pw=pw: crawl_listings(
                                f"L{i+1}",
                                b,
                                chunk,
                                url_queue,
                                seen,
                                sem,
                                completed,
                                delay,
                                stats,
                                cfg,
                                orchestrator=orchestrator,
                                stagger=i * listing_stagger,
                                endpoint_name=endpoint_name,
                                pw=pw,
                                shared=False,
                            ),
                            max_crashes,
                        )
                    )
                    for i in range(len(listing_slots))
                ]

            if use_http:
                # retry через curl_cffi
                retry_snapshot = queue_snapshot(retry_queue)
                if retry_snapshot:
                    log.info(f"[HTTP] retrying {len(retry_snapshot)} failed URLs")
                    retry_as_url = asyncio.Queue()
                    for u in retry_snapshot:
                        await retry_as_url.put(u)
                    while not retry_queue.empty():
                        try:
                            retry_queue.get_nowait()
                        except Exception:
                            break
                    retry_http = asyncio.create_task(
                        run_http_workers(
                            min(n_http, len(retry_snapshot)),
                            retry_as_url,
                            asyncio.Queue(),
                            row_queue,
                            http_pool,
                            stats,
                            cfg,
                            cookies=http_cookies,
                        )
                    )
                    for _ in range(min(n_http, len(retry_snapshot))):
                        await retry_as_url.put(None)
                    await retry_http

            elif session_plan.serial_offer_phase:
                if listing_tasks:
                    await asyncio.gather(*listing_tasks)
                endpoint_name = (
                    offer_slots[0]
                    if offer_slots
                    else (
                        listing_slots[0]
                        if listing_slots
                        else registry.healthy_endpoints()[0]["name"]
                    )
                )
                await _run_serial_offer_retry(
                    browser_pool,
                    endpoint_name,
                    pw,
                    url_queue,
                    retry_queue,
                    row_queue,
                    sem,
                    delay,
                    stats,
                    cfg,
                    orchestrator,
                    max_crashes,
                )
            else:
                offer_stagger = cfg.get("offer_stagger", 0.5)
                offer_tasks = [
                    asyncio.create_task(
                        supervised(
                            f"P{i+1}",
                            lambda i=i, b=browser_pool.get(), endpoint_name=offer_slots[
                                i
                            ], pw=pw: parse_offers(
                                f"P{i+1}",
                                b,
                                url_queue,
                                retry_queue,
                                row_queue,
                                sem,
                                delay,
                                stats,
                                cfg,
                                orchestrator=orchestrator,
                                stagger=i * offer_stagger,
                                endpoint_name=endpoint_name,
                                pw=pw,
                            ),
                            max_crashes,
                        )
                    )
                    for i in range(total_offer)
                ]

                retry_tasks = [
                    asyncio.create_task(
                        supervised(
                            f"R{i+1}",
                            lambda i=i, b=browser_pool.get(), endpoint_name=retry_slots[
                                i
                            ], pw=pw: retry_offers(
                                f"R{i+1}",
                                b,
                                retry_queue,
                                url_queue,
                                row_queue,
                                sem,
                                delay,
                                stats,
                                orchestrator=orchestrator,
                                stagger=i * offer_stagger,
                                cfg=cfg,
                                endpoint_name=endpoint_name,
                                pw=pw,
                            ),
                            max_crashes,
                        )
                    )
                    for i in range(total_retry)
                ]

                if listing_tasks:
                    await asyncio.gather(*listing_tasks)

                for _ in range(total_offer):
                    await url_queue.put(None)
                await asyncio.gather(*offer_tasks)

                if total_retry:
                    for _ in range(total_retry):
                        await retry_queue.put(None)
                    await asyncio.gather(*retry_tasks)
                elif queue_snapshot(retry_queue):
                    endpoint_name = offer_slots[0] if offer_slots else listing_slots[0]
                    await _run_serial_offer_retry(
                        browser_pool,
                        endpoint_name,
                        pw,
                        url_queue,
                        retry_queue,
                        row_queue,
                        sem,
                        delay,
                        stats,
                        cfg,
                        orchestrator,
                        max_crashes,
                    )

            save_checkpoint(
                "cian",
                _build_runtime_checkpoint(
                    completed, url_queue, retry_queue, session_plan, registry
                ),
            )
            await row_queue.put(None)
            await writer_task
        finally:
            if checkpoint_task:
                checkpoint_task.cancel()
            if watchdog_task:
                watchdog_task.cancel()
            if stats_task:
                stats_task.cancel()
            if writer_task and not writer_task.done():
                writer_task.cancel()
            await browser_pool.close_all()
            cleanup_temp_dirs()

    return completed


async def main():
    t0 = time.monotonic()

    cfg = load_scraper_config()
    install_shutdown_handler()

    ensure_vds_tunnel(cfg)
    seen = get_cached_urls(["cian", "cian_history"])
    log.info(f"cache: {len(seen)} urls")

    all_filters = build_filters_from_config(cfg)
    stats = {
        "parsed": 0,
        "captchas": 0,
        "skipped": 0,
        "saved": 0,
        "network_errors": 0,
        "waf_blocks": 0,
    }

    max_restarts = cfg.get("max_restarts", 5)
    cooldown = cfg.get("restart_cooldown", 60)

    for attempt in range(max_restarts + 1):
        if is_shutting_down():
            break

        if attempt > 0:
            log.info(
                f"\n=== RESTART {attempt}/{max_restarts}, cooldown {cooldown}s ==="
            )
            reset_restart()
            await jittered_delay(cooldown * 0.8, cooldown * 1.2)
            seen = get_cached_urls(["cian", "cian_history"])
            log.info(f"cache: {len(seen)} urls")

        await resolve_runtime_endpoints(cfg)
        await _run_session(all_filters, seen, stats, cfg, t0)

        if is_shutting_down():
            break
        if not is_restarting():
            break

    if not is_shutting_down() and not is_restarting():
        clear_checkpoint("cian")
        clear_checkpoint("cian_plan")

    elapsed_min = (time.monotonic() - t0) / 60
    rate = stats["parsed"] / elapsed_min if elapsed_min > 0 else 0
    waf = stats.get("waf_blocks", 0)
    log.info(f"\n{'='*50}")
    log.info(f"  {'STOPPED' if is_shutting_down() else 'DONE'}")
    log.info(f"{'='*50}")
    log.info(f"  parsed:   {stats['parsed']}")
    log.info(f"  saved:    {stats['saved']}")
    log.info(f"  captchas: {stats['captchas']}")
    log.info(f"  waf:      {waf}")
    log.info(f"  skipped:  {stats['skipped']}")
    log.info(f"  net_err:  {stats.get('network_errors', 0)}")
    log.info(f"  time:     {elapsed_min:.1f}min")
    log.info(f"  rate:     {rate:.1f} offers/min")
    log.info(f"{'='*50}")

    stop_vds_tunnel()
    return stats


async def main_loop():
    """бесконечный цикл: прогон -> пауза -> прогон. Ctrl+C останавливает."""
    install_shutdown_handler()
    loop_pause = load_scraper_config().get("loop_pause", 300)
    run = 0

    while not is_shutting_down():
        run += 1
        log.info(f"\n{'#'*50}")
        log.info(f"  RUN #{run}")
        log.info(f"{'#'*50}")

        stats = await main()

        if is_shutting_down():
            break

        log.info(f"\npause {loop_pause}s before next run... (Ctrl+C to stop)")
        try:
            await asyncio.sleep(loop_pause)
        except asyncio.CancelledError:
            break

        # сбрасываем restart flag для чистого старта
        reset_restart()


if __name__ == "__main__":
    asyncio.run(main())
