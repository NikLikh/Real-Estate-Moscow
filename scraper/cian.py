import asyncio
import logging
import random
import sys
import time
from datetime import datetime, timedelta, timezone

from config.settings import load_scraper_config
from db.repository import get_listing_cache, upsert_listings, run_deactivation, insert_daily_snapshot, touch_listings
from scraper.http_offers import run_http_listings, run_http_workers
from proxy_farm import build_proxy_pool
from scraper.planner import build_filters_from_config, http_plan_filters
from scraper.runtime import (
    clear_checkpoint,
    install_shutdown_handler,
    is_restarting,
    is_shutting_down,
    load_checkpoint,
    queue_snapshot,
    request_restart,
    reset_restart,
    save_checkpoint,
    should_stop,
)

log = logging.getLogger("re")


async def jittered_delay(lo, hi):
    await asyncio.sleep(random.uniform(lo, hi))


async def supervised(name, coro_factory, max_crashes):
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


async def flush_rows(row_queue, stats, batch_size=50, flush_interval=5.0):
    buffer = []
    last_flush = time.monotonic()

    async def _flush():
        nonlocal buffer, last_flush
        if not buffer:
            return
        all_rows = []
        for rows, _timings in buffer:
            for r in rows:
                if r.get("price"):
                    all_rows.append(r)

        if all_rows:
            t_db = time.monotonic()
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, upsert_listings, all_rows)
            db_dt = time.monotonic() - t_db
            saved = result["inserted"] + result["updated"]
            stats["saved"] = stats.get("saved", 0) + saved
            stats["inserted"] = stats.get("inserted", 0) + result["inserted"]
            stats["updated"] = stats.get("updated", 0) + result["updated"]
            stats["price_changes"] = stats.get("price_changes", 0) + result["price_changes"]
            log.debug(
                f"[DB] flush {len(all_rows)} rows -> ins={result['inserted']} "
                f"upd={result['updated']} pchg={result['price_changes']} ({db_dt:.1f}s)"
            )
        buffer = []
        last_flush = time.monotonic()

    while True:
        try:
            timeout = max(0.5, flush_interval - (time.monotonic() - last_flush))
            item = await asyncio.wait_for(row_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            await _flush()
            if should_stop():
                break
            continue

        if item is None:
            await _flush()
            break

        rows, timings = item
        buffer.append((rows, timings))

        if len(buffer) >= batch_size:
            await _flush()


async def memory_watchdog(threshold_mb):
    import psutil

    proc = psutil.Process()
    while not should_stop():
        rss = proc.memory_info().rss / 1024 / 1024
        if rss > threshold_mb:
            log.warning(f"[MEM] {rss:.0f}MB > {threshold_mb}MB, requesting restart")
            request_restart(f"memory {rss:.0f}MB")
            break
        await asyncio.sleep(30)


def _build_runtime_checkpoint(completed, url_queue, retry_queue):
    return {
        "completed_filters": list(completed),
        "pending_urls": queue_snapshot(url_queue),
        "retry_urls": queue_snapshot(retry_queue),
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
    completed, url_queue, retry_queue, interval=30
):
    while not should_stop():
        save_checkpoint(
            "cian",
            _build_runtime_checkpoint(completed, url_queue, retry_queue),
        )
        await asyncio.sleep(interval)


async def print_stats_periodically(
    stats, t0, url_queue=None, http_pool=None, interval=5
):
    is_debug = log.isEnabledFor(logging.DEBUG)

    while not should_stop():
        await asyncio.sleep(interval)
        elapsed_min = (time.monotonic() - t0) / 60 if t0 else 1
        rate = stats["parsed"] / elapsed_min if elapsed_min > 0 else 0

        parsed = stats["parsed"]
        ins = stats.get("inserted", 0)
        upd = stats.get("updated", 0)
        pchg = stats.get("price_changes", 0)
        waf = stats.get("waf_blocks", 0)
        cap = stats.get("captchas", 0)
        net = stats.get("network_errors", 0)
        skip = stats.get("skipped", 0)
        sim = stats.get("similar_found", 0)
        touch = stats.get("touched", 0)

        qsize = url_queue.qsize() if url_queue else 0
        alive = http_pool.alive if http_pool else 0
        total_slots = http_pool.slot_count if http_pool else 0

        mem_mb = 0
        try:
            import psutil
            mem_mb = psutil.Process().memory_info().rss / 1024 / 1024
        except Exception:
            pass

        total_planned = stats.get("total_planned", 0)
        if total_planned > 0:
            pct = parsed / total_planned * 100
            progress = f"{pct:05.2f}% ({parsed}/{total_planned})"
        else:
            progress = f"{parsed} parsed"

        # среднее время парсинга, fetch, acquire
        parse_n = stats.get("parse_count", 0)
        avg_parse = stats.get("parse_ms_total", 0) / parse_n if parse_n else 0
        json_pct = stats.get("json_hits", 0) / parse_n * 100 if parse_n else 0
        fetch_n = stats.get("fetch_count", 0)
        avg_fetch = stats.get("fetch_ms_total", 0) / fetch_n if fetch_n else 0
        acq_n = stats.get("acq_count", 0)
        avg_acq = stats.get("acq_ms_total", 0) / acq_n if acq_n else 0
        cycle_n = stats.get("cycle_count", 0)
        avg_cycle = stats.get("cycle_ms_total", 0) / cycle_n if cycle_n else 0

        line = (
            f"[{elapsed_min:.1f}m] {progress} ({rate:.0f}/min) "
            f"| +{ins} new +{upd} upd {pchg} pchg "
            f"| q={qsize} | waf={waf} cap={cap} err={net} skip={skip} "
            f"| {alive}/{total_slots} slots "
            f"| cycle={avg_cycle:.0f}ms fetch={avg_fetch:.0f}ms acq={avg_acq:.0f}ms parse={avg_parse:.0f}ms json={json_pct:.0f}% "
            f"| {mem_mb:.0f}MB"
        )

        if is_debug:
            log.debug(f"[STATS] {line} | sim={sim} touch={touch}")
        else:
            sys.stderr.write(f"\r  {line:<130}")
            sys.stderr.flush()


async def _run_session(all_filters, seen, stats, cfg, t0, listing_cache=None):
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

    http_pool = await build_proxy_pool(cfg)
    n_http = cfg.get("http_offer_workers", 40)
    if http_pool.alive <= 0:
        log.error("[HTTP] no alive slots in pool, aborting session")
        return completed
    log.info(f"[HTTP] pool ready: {http_pool.alive} slots, {n_http} workers")

    max_concurrent = cfg.get("max_concurrent", 15)
    max_crashes = cfg.get("max_worker_crashes", 5)
    mem_threshold = cfg.get("memory_threshold_mb", 2500)
    cfg["_runtime_offer_slots"] = max(1, n_http)
    cfg["_serial_offer_phase"] = False

    url_queue = asyncio.Queue(maxsize=15000)
    retry_queue = asyncio.Queue()
    row_queue = asyncio.Queue()
    restored_counts = await _restore_runtime_queues(
        checkpoint, url_queue, retry_queue, seen
    )

    log.info(f"[PLAN] {n_http} offer + {cfg.get('http_listing_workers', 5)} listing workers")
    if restored_counts != (0, 0):
        log.info(
            f"[PLAN] restored queues: pending={restored_counts[0]} retry={restored_counts[1]}"
        )

    http_cookies = None

    checkpoint_task = None
    writer_task = None
    watchdog_task = None
    stats_task = None
    try:
        filters = []
        if remaining:
            filters, total_offers = await http_plan_filters(
                http_pool, cfg, url_queue=url_queue, seen=seen
            )
            stats["total_planned"] = total_offers
            filters = [f for f in filters if f["label"] not in done_labels]
            log.info(f"after plan: {len(filters)} filters to crawl")

        writer_task = asyncio.create_task(flush_rows(row_queue, stats))
        watchdog_task = asyncio.create_task(memory_watchdog(mem_threshold))
        stats_task = asyncio.create_task(
            print_stats_periodically(stats, t0, url_queue=url_queue, http_pool=http_pool)
        )
        checkpoint_task = asyncio.create_task(
            checkpoint_runtime_periodically(completed, url_queue, retry_queue)
        )

        if filters:
            n_listing = cfg.get("http_listing_workers", 5)

            # offer workers разгребают url_queue пока listing наполняет
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
                    seen=seen,
                )
            )
            await run_http_listings(
                n_listing, filters, url_queue, seen, completed,
                http_pool, stats, cfg, listing_cache=listing_cache,
            )

            # touch листинги, у которых цена совпала с карточкой
            touched_ids = [
                cid for cid, info in listing_cache.items()
                if info.get("_touched")
            ]
            if touched_ids:
                tc = touch_listings(touched_ids)
                log.info(f"[TOUCH] {tc} listings confirmed via listing cards (no full fetch)")

            # листинг закончен, сигналим завершение
            for _ in range(n_http):
                await url_queue.put(None)
            await http_task

        # ретраим
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

        save_checkpoint(
            "cian",
            _build_runtime_checkpoint(completed, url_queue, retry_queue),
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

    return completed


async def main():
    t0 = time.monotonic()
    t0_wall = datetime.now(timezone.utc)

    cfg = load_scraper_config()
    install_shutdown_handler()

    listing_cache = get_listing_cache()
    skip_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    seen = {
        f"https://cian.ru/sale/flat/{cid}/"
        for cid, info in listing_cache.items()
        if info["last_seen_at"] and info["last_seen_at"] >= skip_cutoff
    }
    log.info(f"cache: {len(seen)} urls (of {len(listing_cache)} total)")

    all_filters = build_filters_from_config(cfg)
    stats = {
        "parsed": 0,
        "captchas": 0,
        "skipped": 0,
        "saved": 0,
        "network_errors": 0,
        "waf_blocks": 0,
        "price_changes": 0,
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
            listing_cache = get_listing_cache()
            skip_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
            seen = {
                f"https://cian.ru/sale/flat/{cid}/"
                for cid, last_seen in listing_cache.items()
                if last_seen and last_seen >= skip_cutoff
            }
            log.info(f"cache: {len(seen)} urls (of {len(listing_cache)} total)")

        await _run_session(all_filters, seen, stats, cfg, t0, listing_cache)

        if is_shutting_down():
            break
        if not is_restarting():
            break

    if not is_shutting_down() and not is_restarting():
        clear_checkpoint("cian")
        clear_checkpoint("cian_plan")

    if not is_shutting_down():
        run_deactivation(t0_wall)
        insert_daily_snapshot()

    elapsed_min = (time.monotonic() - t0) / 60
    rate = stats["parsed"] / elapsed_min if elapsed_min > 0 else 0
    waf = stats.get("waf_blocks", 0)
    pch = stats.get("price_changes", 0)
    log.info(f"\n{'='*50}")
    log.info(f"  {'STOPPED' if is_shutting_down() else 'DONE'}")
    log.info(f"{'='*50}")
    log.info(f"  parsed:   {stats['parsed']}")
    log.info(f"  saved:    {stats['saved']}")
    log.info(f"  price_chg:{pch}")
    log.info(f"  captchas: {stats['captchas']}")
    log.info(f"  waf:      {waf}")
    log.info(f"  skipped:  {stats['skipped']}")
    log.info(f"  net_err:  {stats.get('network_errors', 0)}")
    log.info(f"  time:     {elapsed_min:.1f}min")
    log.info(f"  rate:     {rate:.1f} offers/min")
    log.info(f"{'='*50}")

    return stats


async def main_loop():
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

        reset_restart()


if __name__ == "__main__":
    asyncio.run(main())
