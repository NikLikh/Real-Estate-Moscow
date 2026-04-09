import asyncio
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

from config.settings import load_scraper_config
from db.repository import get_listing_cache, upsert_listings, run_deactivation, insert_daily_snapshot, touch_listings, archive_inactive
from scraper.http_offers import run_http_listings, run_http_workers, _cid
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
        cid = _cid(url)
        if cid:
            seen.add(cid)
        await url_queue.put(url)

    for url in retry:
        cid = _cid(url)
        if cid:
            seen.add(cid)
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


def _get_term_width():
    try:
        return os.get_terminal_size().columns
    except Exception:
        return 120


def _bar(pct, width=30):
    pct = max(0, min(100, pct))
    filled = int(width * pct / 100)
    # блочные символы для гладкого прогресс-бара
    blocks = " -=#"
    full = filled
    return "#" * full + "-" * (width - full)


def _fmt_count(n):
    """1234 -> 1.2K, 12345 -> 12.3K, 123456 -> 123K"""
    if n < 1000:
        return str(n)
    if n < 10000:
        return f"{n/1000:.1f}K"
    if n < 100000:
        return f"{n/1000:.1f}K"
    return f"{n/1000:.0f}K"


def _rate_per_minute(history):
    if len(history) < 2:
        return 0
    dt = history[-1][0] - history[0][0]
    if dt < 1:
        return 0
    return (history[-1][1] - history[0][1]) / dt * 60


def _get_mem_mb():
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1024 / 1024
    except Exception:
        return 0


async def print_stats_periodically(
    stats, t0, url_queue=None, http_pool=None, interval=5
):
    q_history = []
    parsed_history = []
    fetch_history = []
    cards_history = []
    prev_lines = 0

    while not should_stop():
        await asyncio.sleep(interval)
        now = time.monotonic()
        elapsed_min = (now - t0) / 60 if t0 else 1
        tw = min(_get_term_width(), 120)

        parsed = stats["parsed"]
        qsize = url_queue.qsize() if url_queue else 0
        alive = http_pool.alive if http_pool else 0
        total_slots = http_pool.slot_count if http_pool else 0
        phase = stats.get("phase", "?")
        total_planned = stats.get("total_planned", 0)
        mem_mb = _get_mem_mb()

        # скользящее окно 60 секунд
        cutoff = now - 60
        q_history.append((now, qsize))
        parsed_history.append((now, parsed))
        fetch_history.append((now, stats.get("fetch_count", 0)))
        cards_history.append((now, stats.get("listing_cards_total", 0)))
        q_history[:] = [(t, v) for t, v in q_history if t >= cutoff]
        parsed_history[:] = [(t, v) for t, v in parsed_history if t >= cutoff]
        fetch_history[:] = [(t, v) for t, v in fetch_history if t >= cutoff]
        cards_history[:] = [(t, v) for t, v in cards_history if t >= cutoff]

        q_rate = _rate_per_minute(q_history)
        parse_rate = _rate_per_minute(parsed_history)
        fetch_rate = _rate_per_minute(fetch_history)
        cards_rate = _rate_per_minute(cards_history)

        ins = stats.get("inserted", 0)
        upd = stats.get("updated", 0)
        pchg = stats.get("price_changes", 0)
        skip = stats.get("skipped", 0)
        no_price = stats.get("no_price", 0)
        region_skip = stats.get("region_skip", 0)
        vpn = stats.get("vpn_blocks", 0)
        bad_st = stats.get("bad_status", 0)
        html_big = stats.get("html_too_large", 0)
        no_cid = stats.get("no_cian_id", 0)
        cap = stats.get("captchas", 0)
        waf = stats.get("waf_blocks", 0)
        net = stats.get("net_errors", 0)
        similar = stats.get("similar_found", 0)
        touched = stats.get("touched", 0)
        lst_cards = stats.get("listing_cards_total", 0)
        lst_skip_url = stats.get("listing_skip_url", 0)
        lst_skip_phrase = stats.get("listing_skip_phrase", 0)
        lst_cached = stats.get("listing_cached", 0)

        cycle_n = stats.get("cycle_count", 0)
        avg_cycle = stats.get("cycle_ms_total", 0) / cycle_n if cycle_n else 0
        fetch_n = stats.get("fetch_count", 0)
        avg_fetch = stats.get("fetch_ms_total", 0) / fetch_n if fetch_n else 0
        parse_n = stats.get("parse_count", 0)
        avg_parse = stats.get("parse_ms_total", 0) / parse_n if parse_n else 0
        json_n = stats.get("json_hits", 0)
        json_pct = json_n * 100 // parse_n if parse_n else 0

        slot_pct = alive * 100 // total_slots if total_slots else 0
        sep = "-" * tw

        lines = []

        if phase == "listing":
            fl_done = stats.get("filters_done", 0)
            fl_total = stats.get("filters_total", 0) or 1
            pct = fl_done / fl_total * 100
            bar_w = max(10, tw - 32)

            # ETA по скорости завершения фильтров
            fl_remaining = fl_total - fl_done
            fl_rate = fl_done / elapsed_min if elapsed_min > 0.1 else 0
            p1_eta = f"{fl_remaining / fl_rate:.0f}m" if fl_rate > 0 else "?"

            lines.append(f"  P1 LISTING  {elapsed_min:.1f}m  ETA {p1_eta}  {fl_done}/{fl_total} filters  {cards_rate:.0f} cards/min")
            lines.append(f"  [{_bar(pct, bar_w)}] {pct:.0f}%")
            lines.append(f"  new: {qsize:,}  cached: {lst_cached:,}  skip: {lst_skip_url + lst_skip_phrase}  touch: {touched}")
            lines.append(f"  slots: {alive}/{total_slots} ({slot_pct}%)  |  waf: {waf}  cap: {cap}  |  {mem_mb:.0f}MB")

        else:
            # phase 2 / retry -- прогресс от реального размера очереди, а не от cian planned
            p2_total = stats.get("p2_total", 0) or total_planned
            pct = parsed / p2_total * 100 if p2_total else 0
            bar_w = max(10, tw - 32)

            remaining = max(0, p2_total - parsed)
            eta = f"{remaining / parse_rate:.0f}m" if parse_rate > 0 else "?"

            phase_label = "P2 OFFERS" if phase == "offers" else phase.upper()
            lines.append(f"  {phase_label}  {elapsed_min:.1f}m  ETA {eta}  {_fmt_count(parsed)}/{_fmt_count(p2_total)}")
            lines.append(f"  [{_bar(pct, bar_w)}] {pct:.1f}%")
            lines.append(f"  +{ins:,} new  +{upd:,} upd  |  {pchg:,} price_chg  |  {parse_rate:.0f}/min")
            lines.append(f"  q: {qsize:,}  |  similar: +{similar}  |  touched: {touched}")
            lines.append(f"  slots: {alive}/{total_slots} ({slot_pct}%)  |  {fetch_rate:.0f} req/min  |  {mem_mb:.0f}MB")

            # ошибки одной строкой, без нулей
            err_parts = []
            if net: err_parts.append(f"net={net:,}")
            if waf: err_parts.append(f"waf={waf}")
            if cap: err_parts.append(f"cap={cap}")
            if vpn: err_parts.append(f"vpn={vpn}")
            if bad_st: err_parts.append(f"status={bad_st}")
            err_line = "  ".join(err_parts) if err_parts else "none"

            skip_parts = []
            if skip: skip_parts.append(f"room/share={skip}")
            if no_price: skip_parts.append(f"no_price={no_price}")
            if region_skip: skip_parts.append(f"region={region_skip}")
            if html_big: skip_parts.append(f"oversized={html_big}")
            if no_cid: skip_parts.append(f"no_id={no_cid}")
            skip_line = "  ".join(skip_parts) if skip_parts else "none"

            lines.append(f"  errors: {err_line}")
            lines.append(f"  skips:  {skip_line}")
            lines.append(f"  timing: cycle={avg_cycle:.0f}  fetch={avg_fetch:.0f}  parse={avg_parse:.0f}ms  json={json_pct}%")

        lines.insert(0, sep)
        lines.append(sep)

        # очищаем предыдущий вывод, рисуем новый
        if prev_lines > 0:
            sys.stderr.write(f"\033[{prev_lines}A")
        for line in lines:
            sys.stderr.write(f"\033[K{line}\n")
        # затираем лишние строки если раньше было больше
        if prev_lines > len(lines):
            for _ in range(prev_lines - len(lines)):
                sys.stderr.write("\033[K\n")
            sys.stderr.write(f"\033[{prev_lines - len(lines)}A")
        sys.stderr.flush()
        prev_lines = len(lines)


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

    url_queue = asyncio.Queue()
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
            stats["filters_total"] = len(filters)
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
            stats["phase"] = "listing"
            log.info(f"[PHASE 1] listing discovery, {len(filters)} filters")
            await run_http_listings(
                cfg.get("http_listing_workers", 25),
                filters, url_queue, seen, completed,
                http_pool, stats, cfg, listing_cache=listing_cache,
            )

            touched_ids = [
                cid for cid, info in listing_cache.items()
                if info.get("_touched")
            ]
            if touched_ids:
                tc = touch_listings(touched_ids)
                log.info(f"[TOUCH] {tc} listings confirmed via listing cards (no full fetch)")

            # запоминаем реальный размер очереди для прогресс-бара P2
            stats["p2_total"] = url_queue.qsize()
            stats["phase"] = "offers"
            log.info(f"[PHASE 2] offer extraction, q={stats['p2_total']}, {n_http} workers")

            # phase 2: offers -- все слоты обрабатывают офферы
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
            for _ in range(n_http):
                await url_queue.put(None)
            await http_task

        # многораундовый retry -- добиваем все ошибки
        max_rounds = cfg.get("max_retry_rounds", 3)
        for rnd in range(1, max_rounds + 1):
            retry_snapshot = queue_snapshot(retry_queue)
            if not retry_snapshot:
                break

            stats["phase"] = f"retry R{rnd}"
            n_workers = min(n_http, max(4, len(retry_snapshot) // 2))
            log.info(f"[RETRY] round {rnd}/{max_rounds}: {len(retry_snapshot)} URLs, {n_workers} workers")

            retry_as_url = asyncio.Queue()
            for u in retry_snapshot:
                await retry_as_url.put(u)
            # очищаем старую retry_queue, новые ошибки пойдут в неё же
            while not retry_queue.empty():
                try:
                    retry_queue.get_nowait()
                except Exception:
                    break

            retry_http = asyncio.create_task(
                run_http_workers(
                    n_workers,
                    retry_as_url,
                    retry_queue,
                    row_queue,
                    http_pool,
                    stats,
                    cfg,
                    cookies=http_cookies,
                )
            )
            for _ in range(n_workers):
                await retry_as_url.put(None)
            await retry_http

            # если очередь не уменьшилась существенно, дальше бесполезно
            new_size = retry_queue.qsize()
            if new_size >= len(retry_snapshot) * 0.9:
                log.info(f"[RETRY] round {rnd} barely helped ({len(retry_snapshot)} -> {new_size}), stopping")
                break

        final_failed = retry_queue.qsize()
        if final_failed:
            stats["final_failed"] = final_failed
            log.info(f"[RETRY] {final_failed} URLs failed after all rounds")

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
    seen = set(listing_cache.keys())
    log.info(f"cache: {len(seen)} ids")

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
            seen = set(listing_cache.keys())
            log.info(f"cache: {len(seen)} ids")

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
        archive_inactive()
        insert_daily_snapshot()

    elapsed_min = (time.monotonic() - t0) / 60
    rate = stats["parsed"] / elapsed_min if elapsed_min > 0 else 0
    total_planned = stats.get("total_planned", 0)
    parsed = stats["parsed"]
    saved = stats.get("saved", 0)
    ins = stats.get("inserted", 0)
    upd = stats.get("updated", 0)

    w = 56
    log.info(f"\n{'=' * w}")
    log.info(f"  {'STOPPED' if is_shutting_down() else 'DONE'}  |  {elapsed_min:.1f}min  |  {rate:.0f}/min")
    log.info(f"{'=' * w}")

    # результаты
    p2_total = stats.get("p2_total", 0) or total_planned
    p2_pct = parsed * 100 // max(p2_total, 1)
    log.info(f"  cian total:  {total_planned:>8,}  (from filter counts)")
    log.info(f"  queued:      {p2_total:>8,}  (actual URLs for P2)")
    log.info(f"  parsed:      {parsed:>8,}  ({p2_pct}% of queued)")
    log.info(f"  saved:       {saved:>8,}  (+{ins:,} new  +{upd:,} upd)")
    log.info(f"  price_chg:   {stats.get('price_changes', 0):>8,}")
    log.info(f"  touched:     {stats.get('touched', 0):>8,}")
    log.info(f"  similar:     {stats.get('similar_found', 0):>8,}")
    log.info(f"{'-' * w}")

    # listing фаза
    lst_cards = stats.get("listing_cards_total", 0)
    lst_skip_url = stats.get("listing_skip_url", 0)
    lst_skip_phrase = stats.get("listing_skip_phrase", 0)
    lst_skip = lst_skip_url + lst_skip_phrase
    lst_cached = stats.get("listing_cached", 0)
    if lst_cards:
        log.info(f"  listing cards:  {lst_cards:,} total")
        log.info(f"    cached:      {lst_cached:>7,}  (already in DB)")
        log.info(f"    skipped:     {lst_skip:>7,}  (url={lst_skip_url} phrase={lst_skip_phrase})")
    log.info(f"{'-' * w}")

    # потери на offer фазе
    skip = stats.get("skipped", 0)
    no_price = stats.get("no_price", 0)
    region_skip = stats.get("region_skip", 0)
    html_big = stats.get("html_too_large", 0)
    no_cid = stats.get("no_cian_id", 0)
    total_skips = skip + no_price + region_skip + html_big + no_cid
    log.info(f"  offer skips: {total_skips:>7,}")
    if skip: log.info(f"    room/share:  {skip:>7,}")
    if no_price: log.info(f"    no_price:    {no_price:>7,}")
    if region_skip: log.info(f"    region:      {region_skip:>7,}")
    if html_big: log.info(f"    oversized:   {html_big:>7,}")
    if no_cid: log.info(f"    no_cian_id:  {no_cid:>7,}")
    log.info(f"{'-' * w}")

    # сетевые ошибки
    net = stats.get("net_errors", 0)
    waf = stats.get("waf_blocks", 0)
    cap = stats.get("captchas", 0)
    vpn = stats.get("vpn_blocks", 0)
    bad_st = stats.get("bad_status", 0)
    final_failed = stats.get("final_failed", 0)
    total_errors = net + waf + cap + vpn + bad_st
    log.info(f"  net errors:  {total_errors:>7,}  (final_failed={final_failed})")
    if net: log.info(f"    network:     {net:>7,}")
    if waf: log.info(f"    waf:         {waf:>7,}")
    if cap: log.info(f"    captcha:     {cap:>7,}")
    if vpn: log.info(f"    vpn_block:   {vpn:>7,}")
    if bad_st: log.info(f"    bad_status:  {bad_st:>7,}")
    log.info(f"{'=' * w}")

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
