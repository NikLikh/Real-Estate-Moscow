"""Скрапер cian.ru"""

import asyncio
import logging
import time
from datetime import datetime

from bs4 import BeautifulSoup
from patchright.async_api import async_playwright

from config.settings import load_scraper_config
from db.repository import get_cached_urls, save_rows
from scraper.browser import (
    OFFER_EXTRA_BLOCKED,
    AdaptiveDelay,
    SessionIdentity,
    apply_cdp_blocking,
    create_stealth_context,
    detect_captcha,
    detect_vpn_block,
    detect_waf_rate_limit,
    handle_captcha,
    handle_vpn_block,
    humanize,
    jittered_delay,
    launch_browser_pool,
    warmup_session,
)
from scraper.parsers import parse_offer_page
from scraper.planner import build_filters_from_config, plan_filters
from scraper.proxy import ProxyPool, auto_discover, ensure_vds_tunnel, stop_vds_tunnel
from scraper.runtime import (
    clear_checkpoint,
    install_shutdown_handler,
    is_restarting,
    is_shutting_down,
    load_checkpoint,
    request_restart,
    reset_restart,
    save_checkpoint,
    save_dead_letter,
    should_stop,
)

log = logging.getLogger("re")

# при этих ошибках HTTP/2 соединение мертво, нужна ротация контекста
NETWORK_ERRORS = (
    "ERR_CONNECTION_TIMED_OUT",
    "ERR_HTTP2_PING_FAILED",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_CLOSED",
    "ERR_NETWORK_CHANGED",
    "ERR_SOCKET_NOT_CONNECTED",
)


async def throttled_goto(page, url, sem, timeout=120000, throttle=None):
    # goto через семафор + rate limiter, возвращает (ok, dt)
    if throttle:
        await throttle.wait()
    async with sem:
        t0 = time.monotonic()
        try:
            await page.goto(url, timeout=timeout, wait_until="domcontentloaded")
            return True, time.monotonic() - t0
        except Exception as e:
            msg = str(e)
            log.debug(f"    goto failed: {msg[:60]}")
            dt = time.monotonic() - t0
            if any(err in msg for err in NETWORK_ERRORS):
                return "network", dt
            return False, dt


async def _check_page(page, delay) -> bool:
    if not await handle_vpn_block(page):
        return False
    if not await handle_captcha(page):
        delay.report_captcha()
        return False
    return True


async def _make_context(browser, block_extra=False, endpoint=None, identity=None, do_warmup=True):
    proxy = endpoint.get("proxy") if endpoint else None
    ctx = await create_stealth_context(browser, proxy=proxy, identity=identity)
    # без referer прямой визит offer-страницы палит бота
    await ctx.set_extra_http_headers({
        "Referer": "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat",
    })
    page = await ctx.new_page()
    extra = OFFER_EXTRA_BLOCKED if block_extra else ()
    cdp = await apply_cdp_blocking(page, extra_patterns=extra)
    if do_warmup:
        await warmup_session(page)
    return ctx, page, cdp


async def _handle_offer_captcha(name, ctx, url, throttle=None):
    # решаем капчу во временной странице с JS, cookies обновляются в контексте
    if throttle:
        await throttle.wait()
    cap_page = await ctx.new_page()
    await apply_cdp_blocking(cap_page)
    try:
        await cap_page.goto(url, timeout=30000, wait_until="domcontentloaded")
        ok = await handle_captcha(cap_page)
        if ok:
            log.info(f"[{name}] captcha solved via temp page")
        return ok
    except Exception as e:
        log.debug(f"[{name}] captcha temp page failed: {e}")
        return False
    finally:
        try:
            await cap_page.close()
        except Exception:
            pass


async def _close_ctx(ctx, page):
    try:
        await page.close()
    except Exception:
        pass
    try:
        await ctx.close()
    except Exception:
        pass


class EndpointSession:
    """browser context + endpoint, ротация при WAF/network/captcha"""

    def __init__(self, name, browser, pool, *, block_extra=True, identity=None):
        self.name = name
        self._browser = browser
        self._pool = pool
        self._block_extra = block_extra
        self._identity = identity or SessionIdentity()
        self.ctx = None
        self.page = None
        self.cdp = None
        self.ep = pool.get_endpoint() if pool else {"name": "direct", "proxy": None, "throttle": None}
        self.throttle = self.ep.get("throttle")
        self._req_count = 0

    async def open(self):
        self.ctx, self.page, self.cdp = await _make_context(
            self._browser, block_extra=self._block_extra,
            endpoint=self.ep, identity=self._identity,
        )
        self._req_count = 0

    async def close(self):
        if self.ctx and self.page:
            await _close_ctx(self.ctx, self.page)

    async def rotate(self, reason=""):
        # смена endpoint + перезапуск контекста
        await self.close()
        if self._pool:
            self.ep = self._pool.get_endpoint()
        self.throttle = self.ep.get("throttle")
        if reason:
            log.info(f"[{self.name}] {reason}, rotating to {self.ep['name']}")
        await self.open()

    async def restart_batch(self, cooldown):
        # перезапуск контекста на том же endpoint
        await self.close()
        await jittered_delay(*cooldown)
        await self.open()

    def tick(self):
        self._req_count += 1

    async def maybe_restart_batch(self, batch_size, cooldown):
        self._req_count += 1
        if self._req_count > batch_size:
            log.info(f"[{self.name}] batch done ({batch_size}), restarting context")
            await self.restart_batch(cooldown)

    async def goto(self, url, sem, timeout=30000):
        return await throttled_goto(self.page, url, sem, timeout=timeout, throttle=self.throttle)

    async def handle_captcha(self, url):
        return await _handle_offer_captcha(self.name, self.ctx, url, self.throttle)

    def report_waf(self):
        if self._pool:
            self._pool.report_waf(self.ep["name"])

    def report_success(self):
        if self._pool:
            self._pool.report_success(self.ep["name"])


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
    name, browser, filters, url_queue, seen, sem, completed, delay, stats, cfg, pool=None, stagger=0,
):
    # ходит по листингам и собирает url-ы офферов в очередь
    max_pages = cfg.get("max_pages", 54)
    max_cached = cfg.get("max_cached_pages", 3)
    min_new = cfg.get("min_new_first_page", 5)
    n_offer = cfg.get("offer_workers", 8)
    skip_urls = cfg.get("skip_url_parts", [])
    skip_phrases = cfg.get("skip_phrases", [])
    listing_batch = cfg.get("listing_batch_size", 5)
    batch_cooldown = cfg.get("batch_cooldown", [8.0, 15.0])

    if stagger:
        await asyncio.sleep(stagger)

    s = EndpointSession(name, browser, pool)
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
                threshold = 5 * n_offer
                while url_queue.qsize() > threshold and not should_stop():
                    await asyncio.sleep(2)

                if delay.under_pressure:
                    await jittered_delay(3.0, 6.0)

                url = f"{filt['url']}&p={pg}"
                log.info(f"[{name}] {label} p.{pg}/{max_pages}")

                result, _ = await s.goto(url, sem)
                if result == "network":
                    await s.rotate("network error")
                    consecutive_page_fails += 1
                    await jittered_delay(3.0, 6.0)
                    continue
                if not result:
                    consecutive_page_fails += 1
                    continue

                if await detect_waf_rate_limit(s.page):
                    stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
                    consecutive_waf += 1
                    log.warning(f"[{name}] WAF #{consecutive_waf} on {s.ep['name']}")
                    s.report_waf()
                    if consecutive_waf >= 6:
                        request_restart(f"WAF x{consecutive_waf}")
                        break
                    await s.rotate()
                    await jittered_delay(2.0, 5.0)
                    continue

                consecutive_waf = 0
                s.report_success()

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
                        await s.rotate("captcha failed")
                        await delay.wait()
                        continue

                if await detect_vpn_block(s.page):
                    await s.rotate("VPN block")
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
                        log.info(f"[{name}] no cards on p.{pg}, title='{title[:60]}', end of filter")
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
                save_checkpoint("cian", {"completed_filters": completed})
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


async def _parse_and_save(name, page, url, row_queue, stats, t_start=None, goto_dt=0, check_dt=0, max_retries=5):
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
    name, browser, url_queue, retry_queue, row_queue, sem, delay, stats, cfg, pool=None, stagger=0,
):
    if stagger:
        await asyncio.sleep(stagger)

    batch_size = cfg.get("batch_size", 25)
    batch_cooldown = cfg.get("batch_cooldown", [2.0, 4.0])
    s = EndpointSession(name, browser, pool)
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
                await s.rotate("network error")
                await retry_queue.put(url)
                await jittered_delay(2.0, 4.0)
                continue
            if not result:
                await retry_queue.put(url)
                await jittered_delay(0.5, 1.0)
                continue

            if await detect_waf_rate_limit(s.page):
                stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
                consecutive_waf += 1
                log.warning(f"[{name}] WAF #{consecutive_waf} on {s.ep['name']}")
                await url_queue.put(url)
                s.report_waf()
                if consecutive_waf >= 6:
                    request_restart(f"WAF x{consecutive_waf}")
                    break
                await s.rotate()
                await jittered_delay(2.0, 5.0)
                continue

            consecutive_waf = 0
            s.report_success()

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
                    await s.rotate("captcha failed")
                    await retry_queue.put(url)
                    await jittered_delay(2.0, 4.0)
                    continue
            check_dt = time.monotonic() - t_check

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


async def retry_offers(name, browser, retry_queue, url_queue, row_queue, sem, delay, stats, pool=None, stagger=0, cfg=None):
    if stagger:
        await asyncio.sleep(stagger)

    cfg = cfg or {}
    batch_size = cfg.get("batch_size", 25)
    batch_cooldown = cfg.get("batch_cooldown", [2.0, 4.0])

    s = EndpointSession(name, browser, pool)
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
                await s.rotate("network error")
                consecutive_fails = 0
                await jittered_delay(3.0, 6.0)
                continue
            if not result:
                consecutive_fails += 1
                stats["skipped"] += 1
                if consecutive_fails >= 5:
                    await s.rotate(f"{consecutive_fails} fails")
                    consecutive_fails = 0
                    await jittered_delay(5.0, 10.0)
                continue

            if await detect_waf_rate_limit(s.page):
                stats["waf_blocks"] = stats.get("waf_blocks", 0) + 1
                consecutive_waf += 1
                log.warning(f"[{name}] WAF #{consecutive_waf} on {s.ep['name']}")
                await url_queue.put(url)
                s.report_waf()
                if consecutive_waf >= 6:
                    request_restart(f"WAF x{consecutive_waf}")
                    break
                await s.rotate()
                await jittered_delay(2.0, 5.0)
                continue

            consecutive_waf = 0
            s.report_success()

            if await detect_captcha(s.page, url_only=True):
                stats["captchas"] += 1
                delay.captcha_enter()
                try:
                    ok = await s.handle_captcha(url)
                finally:
                    delay.captcha_exit()
                if not ok:
                    await s.rotate("captcha failed")
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


async def print_stats_periodically(stats, t0, interval=300):
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


async def _run_session(all_filters, seen, stats, cfg, t0, proxy_pool=None):
    checkpoint = load_checkpoint("cian")
    done_labels = set(checkpoint.get("completed_filters", [])) if checkpoint else set()
    if done_labels:
        log.info(f"checkpoint: {len(done_labels)} filters done")

    remaining = [f for f in all_filters if f["label"] not in done_labels]
    completed = list(done_labels)
    log.info(f"remaining: {len(remaining)} of {len(all_filters)}")

    if not remaining:
        log.info("all filters done!")
        clear_checkpoint("cian")
        return completed

    n_listing = cfg.get("listing_workers", 1)
    n_offer = cfg.get("offer_workers", 4)
    n_retry = cfg.get("retry_workers", 2)
    max_concurrent = cfg.get("max_concurrent", 10)
    queue_max = cfg.get("url_queue_max", 300)
    max_crashes = cfg.get("max_worker_crashes", 5)
    mem_threshold = cfg.get("memory_threshold_mb", 2500)
    n_browsers = cfg.get("browser_pool_size", 6)

    # offer_workers * кол-во endpoints = реальное число воркеров
    n_endpoints = len(proxy_pool.get_healthy()) if proxy_pool else 1
    total_offer = n_offer * n_endpoints
    total_retry = n_retry

    sem = asyncio.Semaphore(max_concurrent)
    url_queue = asyncio.Queue(maxsize=queue_max)
    retry_queue = asyncio.Queue()
    row_queue = asyncio.Queue()
    delay = AdaptiveDelay()

    log.info(
        f"workers: {n_listing} listing + {total_offer} offer ({n_offer}x{n_endpoints} ep) "
        f"+ {total_retry} retry, sem={max_concurrent}, browsers={n_browsers}"
    )

    async with async_playwright() as pw:
        browser_pool = await launch_browser_pool(
            pw, n_browsers, headless=cfg.get("headless", True)
        )

        try:
            # planner параллельно проверяет фильтры через N страниц из пула
            filters = await plan_filters(browser_pool, sem, cfg, proxy_pool=proxy_pool)

            filters = [f for f in filters if f["label"] not in done_labels]
            log.info(f"after plan: {len(filters)} filters to crawl")

            # делим фильтры между листинг-воркерами чтобы не дублировать
            filter_chunks = [filters[i::n_listing] for i in range(n_listing)]
            listing_stagger = cfg.get("listing_stagger", 3.0)
            listing_tasks = [
                asyncio.create_task(
                    supervised(
                        f"L{i+1}",
                        lambda i=i, b=browser_pool.get(), chunk=filter_chunks[i]: crawl_listings(
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
                            pool=proxy_pool,
                            stagger=i * listing_stagger,
                        ),
                        max_crashes,
                    )
                )
                for i in range(n_listing)
            ]

            offer_stagger = cfg.get("offer_stagger", 0.5)
            offer_tasks = [
                asyncio.create_task(
                    supervised(
                        f"P{i+1}",
                        lambda i=i, b=browser_pool.get(): parse_offers(
                            f"P{i+1}",
                            b,
                            url_queue,
                            retry_queue,
                            row_queue,
                            sem,
                            delay,
                            stats,
                            cfg,
                            pool=proxy_pool,
                            stagger=i * offer_stagger,
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
                        lambda i=i, b=browser_pool.get(): retry_offers(
                            f"R{i+1}",
                            b,
                            retry_queue,
                            url_queue,
                            row_queue,
                            sem,
                            delay,
                            stats,
                            pool=proxy_pool,
                            stagger=i * offer_stagger,
                            cfg=cfg,
                        ),
                        max_crashes,
                    )
                )
                for i in range(total_retry)
            ]

            writer_task = asyncio.create_task(flush_rows(row_queue, stats))
            watchdog_task = asyncio.create_task(memory_watchdog(mem_threshold))
            stats_task = asyncio.create_task(
                print_stats_periodically(stats, t0)
            )

            await asyncio.gather(*listing_tasks)

            for _ in range(total_offer):
                await url_queue.put(None)
            await asyncio.gather(*offer_tasks)

            for _ in range(total_retry):
                await retry_queue.put(None)
            await asyncio.gather(*retry_tasks)

            await row_queue.put(None)
            await writer_task

            watchdog_task.cancel()
            stats_task.cancel()

        finally:
            await browser_pool.close_all()

    return completed


async def main():
    t0 = time.monotonic()

    cfg = load_scraper_config()
    install_shutdown_handler()

    # поднимаем VDS tunnel если настроен, затем находим endpoints
    ensure_vds_tunnel(cfg)
    if cfg.get("auto_discover", True):
        discovered = await auto_discover(cfg)
        cfg["endpoints"] = discovered

    seen = get_cached_urls(["cian", "cian_history"])
    log.info(f"cache: {len(seen)} urls")

    all_filters = build_filters_from_config(cfg)
    stats = {"parsed": 0, "captchas": 0, "skipped": 0, "saved": 0, "network_errors": 0, "waf_blocks": 0}

    max_restarts = cfg.get("max_restarts", 5)
    cooldown = cfg.get("restart_cooldown", 60)

    async with ProxyPool(cfg) as proxy_pool:
        for attempt in range(max_restarts + 1):
            if is_shutting_down():
                break

            if attempt > 0:
                log.info(f"\n=== RESTART {attempt}/{max_restarts}, cooldown {cooldown}s ===")
                reset_restart()
                await jittered_delay(cooldown * 0.8, cooldown * 1.2)
                seen = get_cached_urls(["cian", "cian_history"])
                log.info(f"cache: {len(seen)} urls")

            await _run_session(all_filters, seen, stats, cfg, t0, proxy_pool=proxy_pool)

            if is_shutting_down():
                break
            if not is_restarting():
                break

    if not is_shutting_down() and not is_restarting():
        clear_checkpoint("cian")
        clear_checkpoint("cian_plan")

    mode = "STOPPED" if is_shutting_down() else "DONE"
    elapsed_min = (time.monotonic() - t0) / 60
    rate = stats["parsed"] / elapsed_min if elapsed_min > 0 else 0
    waf = stats.get("waf_blocks", 0)
    log.info(f"\n{'='*50}")
    log.info(f"  {mode}")
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
    if is_shutting_down():
        log.info("will resume from checkpoint")

    stop_vds_tunnel()


if __name__ == "__main__":
    asyncio.run(main())
