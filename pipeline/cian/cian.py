import asyncio
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

from config.settings import load_scraper_config
from pipeline.core.raw_repo import get_current_state, insert_observations, insert_run_stats
from pipeline.cian.http_offers import run_http_listings, run_http_workers, run_http_zhk
from pipeline.cian.parsers import extract_cian_id
from pipeline.cian.proxy_farm import build_proxy_pool, wait_for_pool
from pipeline.cian.proxy_farm.refresher import run_refresher
from pipeline.cian.planner import build_filters_from_config, http_plan_filters
from pipeline.cian.runtime import (
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

_run_id_env = os.getenv("SCRAPE_RUN_ID")
SCRAPE_RUN_ID = int(_run_id_env) if _run_id_env else None


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
                    if r.get("_html_price_history"):
                        r["payload"] = {"price_history": r["_html_price_history"]}
                    all_rows.append(r)

        if all_rows:
            t_db = time.monotonic()
            loop = asyncio.get_event_loop()
            try:
                n = await loop.run_in_executor(None, insert_observations, all_rows, SCRAPE_RUN_ID)
            except Exception as e:
                n = 0
                log.error(f"[DB] flush потерян ({len(all_rows)} строк): {e}")
            db_dt = time.monotonic() - t_db
            stats["saved"] = stats.get("saved", 0) + n
            stats["db_dropped"] = stats.get("db_dropped", 0) + (len(all_rows) - n)
            log.debug(f"[DB] flush {len(all_rows)} rows -> obs={n} ({db_dt:.1f}s)")
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


async def write_presence(listing_cache):
    today = datetime.now(timezone.utc).date()
    rows = []
    for cid, info in listing_cache.items():
        if not info.get("_touched") or info.get("_repriced") or info.get("_presence_written"):
            continue
        last_seen = info.get("last_seen_at")
        if last_seen and last_seen.astimezone(timezone.utc).date() >= today:
            continue
        price = info.get("_card_price") or info.get("price")
        if price is None:
            continue
        info["_presence_written"] = True
        rows.append({"cian_id": cid, "price": price, "deal_type": info.get("_deal")})

    if not rows:
        return 0

    loop = asyncio.get_event_loop()
    written = 0
    for i in range(0, len(rows), 20000):
        written += await loop.run_in_executor(
            None, insert_observations, rows[i:i + 20000], SCRAPE_RUN_ID
        )
    return written


async def pool_watchdog(pool, min_alive, grace):
    need = max(2, grace // 30)
    samples = []
    while not should_stop():
        await asyncio.sleep(30)
        samples.append(pool.alive)
        if len(samples) > need:
            samples.pop(0)
        if len(samples) < need:
            continue
        if sum(samples) == 0:
            log.warning(f"[POOL] {grace}s без единого живого слота, подача не справляется")
            request_restart("подача прокси остановилась")
            break


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
        cid = extract_cian_id(url)
        if cid:
            seen.add(cid)
        await url_queue.put(url)

    for url in retry:
        cid = extract_cian_id(url)
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
    return "#" * filled + "-" * (width - filled)


def _fmt_count(n):
    if n < 1000:
        return str(n)
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
        touched = stats.get("price_changes", 0)
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

            fl_remaining = fl_total - fl_done
            fl_rate = fl_done / elapsed_min if elapsed_min > 0.1 else 0
            p1_eta = f"{fl_remaining / fl_rate:.0f}m" if fl_rate > 0 else "?"

            lines.append(f"  P1 LISTING  {elapsed_min:.1f}m  ETA {p1_eta}  {fl_done}/{fl_total} filters  {cards_rate:.0f} cards/min")
            lines.append(f"  [{_bar(pct, bar_w)}] {pct:.0f}%")
            lines.append(f"  new: {qsize:,}  cached: {lst_cached:,}  skip: {lst_skip_url + lst_skip_phrase}  repriced: {touched}  incomplete: {stats.get('filters_incomplete', 0)}")
            pool_dbg = http_pool.debug_state() if http_pool else ""
            lines.append(f"  slots: {alive}/{total_slots} ({slot_pct}%)  |  waf: {waf}  cap: {cap}  net: {net}  status: {bad_st}  |  {mem_mb:.0f}MB")
            lines.append(f"  pool: {pool_dbg}  |  empty: {stats.get('empty_pages', 0)}  |  q_listing: {stats.get('listing_inflight', 0)}")

        elif phase == "zhk":
            zhk_done = stats.get("zhk_done", 0)
            zhk_total = stats.get("zhk_total", 0) or 1
            zhk_new = stats.get("zhk_new", 0)
            zhk_capped = stats.get("zhk_capped", 0)
            pct = zhk_done / zhk_total * 100
            bar_w = max(10, tw - 32)

            zhk_rate = zhk_done / elapsed_min if elapsed_min > 0.1 else 0
            zhk_eta = f"{(zhk_total - zhk_done) / zhk_rate:.0f}m" if zhk_rate > 0 else "?"

            lines.append(f"  P1b ЖК-DRILL  {elapsed_min:.1f}m  ETA {zhk_eta}  {zhk_done:,}/{zhk_total:,} ЖК")
            lines.append(f"  [{_bar(pct, bar_w)}] {pct:.1f}%")
            lines.append(f"  +{zhk_new:,} квартир в очередь  |  capped: {zhk_capped:,}  |  q: {qsize:,}")
            lines.append(f"  slots: {alive}/{total_slots} ({slot_pct}%)  |  {mem_mb:.0f}MB")
            err_parts = []
            if net: err_parts.append(f"net={net:,}")
            if waf: err_parts.append(f"waf={waf}")
            if cap: err_parts.append(f"cap={cap}")
            if bad_st: err_parts.append(f"status={bad_st}")
            lines.append(f"  errors: {'  '.join(err_parts) if err_parts else 'none'}")

        else:
            p2_total = stats.get("p2_total", 0) or total_planned
            pct = parsed / p2_total * 100 if p2_total else 0
            bar_w = max(10, tw - 32)

            remaining = max(0, p2_total - parsed)
            eta = f"{remaining / parse_rate:.0f}m" if parse_rate > 0 else "?"

            phase_label = "P2 OFFERS" if phase == "offers" else phase.upper()
            lines.append(f"  {phase_label}  {elapsed_min:.1f}m  ETA {eta}  {_fmt_count(parsed)}/{_fmt_count(p2_total)}")
            lines.append(f"  [{_bar(pct, bar_w)}] {pct:.1f}%")
            lines.append(f"  saved: {stats.get('saved', 0):,}  |  {parse_rate:.0f}/min")
            lines.append(f"  q: {qsize:,}  |  similar: +{similar}  |  repriced: {touched}")
            lines.append(f"  slots: {alive}/{total_slots} ({slot_pct}%)  |  {fetch_rate:.0f} req/min  |  {mem_mb:.0f}MB")

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

        if prev_lines > 0:
            sys.stderr.write(f"\033[{prev_lines}A")
        for line in lines:
            sys.stderr.write(f"\033[K{line}\n")
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
    min_alive = cfg.get("min_alive_slots", 5)
    supply_task = asyncio.create_task(run_refresher(http_pool, cfg))
    if not await wait_for_pool(http_pool, min_alive, cfg.get("pool_warmup_wait", 300)):
        supply_task.cancel()
        log.error(f"[HTTP] pool {http_pool.alive} slots < {min_alive}, aborting session")
        request_restart(f"пул {http_pool.alive} слотов на старте")
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

    checkpoint_task = None
    writer_task = None
    watchdog_task = None
    pool_task = None
    stats_task = None
    refresher_task = supply_task
    try:
        filters = []
        if remaining:
            filters, total_offers = await http_plan_filters(
                http_pool, cfg, url_queue=url_queue, seen=seen
            )
            stats["total_planned"] = total_offers
            stats["plan_size"] = len(filters)
            filters = [f for f in filters if f["label"] not in done_labels]
            stats["filters_total"] = len(filters)
            log.info(f"after plan: {len(filters)} filters to crawl")

        writer_task = asyncio.create_task(flush_rows(row_queue, stats))
        watchdog_task = asyncio.create_task(memory_watchdog(mem_threshold))
        pool_task = asyncio.create_task(
            pool_watchdog(
                http_pool,
                cfg.get("min_alive_slots", 5),
                cfg.get("pool_low_grace", 300),
            )
        )
        stats_task = asyncio.create_task(
            print_stats_periodically(stats, t0, url_queue=url_queue, http_pool=http_pool)
        )
        checkpoint_task = asyncio.create_task(
            checkpoint_runtime_periodically(completed, url_queue, retry_queue)
        )

        zhk_ids = set()
        saved_zhk = (load_checkpoint("cian_zhk") or {}).get("ids") or []
        limit = cfg.get("zhk_max_per_run", 800)
        if saved_zhk and not stats.get("zhk_run") and not should_stop():
            stats["zhk_run"] = True
            stats["phase"] = "zhk"
            drill_ids = random.sample(saved_zhk, limit) if limit and len(saved_zhk) > limit else list(saved_zhk)
            log.info(f"[PHASE 0] ЖК-drill: {len(drill_ids)} из {len(saved_zhk)} ЖК")
            await run_http_zhk(drill_ids, url_queue, seen, http_pool, stats, cfg,
                               row_queue=row_queue, listing_cache=listing_cache)
            log.info(f"[PHASE 0] +{stats.get('zhk_new', 0)} квартир, capped={stats.get('zhk_capped', 0)}")
            tc = await write_presence(listing_cache)
            if tc:
                stats["presence"] = stats.get("presence", 0) + tc
                log.info(f"[OBS] {tc} presence observations from ЖК-drill")

        if filters:
            stats["phase"] = "listing"
            log.info(f"[PHASE 1] listing discovery, {len(filters)} filters")
            await run_http_listings(
                cfg.get("http_listing_workers", 25),
                filters, url_queue, seen, completed,
                http_pool, stats, cfg, listing_cache=listing_cache, zhk_ids=zhk_ids,
                row_queue=row_queue,
            )

            tc = await write_presence(listing_cache)
            if tc:
                stats["presence"] = stats.get("presence", 0) + tc
                log.info(f"[OBS] {tc} presence observations from listing cards")

            if zhk_ids:
                save_checkpoint("cian_zhk", {"ids": sorted(zhk_ids | set(saved_zhk))})
                log.info(f"[ЖК] в чекпоинте {len(zhk_ids | set(saved_zhk))} ЖК")

        deferred = []
        p2_cap = cfg.get("p2_max_urls", 0)
        if p2_cap:
            while url_queue.qsize() > p2_cap:
                deferred.append(url_queue.get_nowait())
            stats["p2_deferred"] = len(deferred)

        if url_queue.qsize() and not should_stop():
            stats["p2_total"] = url_queue.qsize()
            stats["phase"] = "offers"
            log.info(f"[PHASE 2] offer extraction, q={stats['p2_total']}, {n_http} workers")

            http_task = asyncio.create_task(
                run_http_workers(
                    n_http,
                    url_queue,
                    retry_queue,
                    row_queue,
                    http_pool,
                    stats,
                    cfg,
                    seen=seen,
                )
            )
            for _ in range(n_http):
                await url_queue.put(None)
            await http_task

        for u in deferred:
            await url_queue.put(u)

        max_rounds = cfg.get("max_retry_rounds", 3)
        for rnd in range(1, max_rounds + 1):
            if should_stop():
                break
            retry_snapshot = queue_snapshot(retry_queue)
            if not retry_snapshot:
                break

            stats["phase"] = f"retry R{rnd}"
            n_workers = min(n_http, max(4, len(retry_snapshot) // 2))
            log.info(f"[RETRY] round {rnd}/{max_rounds}: {len(retry_snapshot)} URLs, {n_workers} workers")

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
                    n_workers,
                    retry_as_url,
                    retry_queue,
                    row_queue,
                    http_pool,
                    stats,
                    cfg,
                    seen=seen,
                )
            )
            for _ in range(n_workers):
                await retry_as_url.put(None)
            await retry_http
            for u in queue_snapshot(retry_as_url):
                await retry_queue.put(u)

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
        if pool_task:
            pool_task.cancel()
        if stats_task:
            stats_task.cancel()
        if refresher_task:
            refresher_task.cancel()
        if writer_task and not writer_task.done():
            writer_task.cancel()

    if http_pool:
        stats["pool_slots"] = http_pool.slot_count
        stats["pool_alive"] = http_pool.alive
        log.info(f"{'-' * 56}")
        log.info("  источники прокси (alive/total  ok  net  waf  cap  quar  spent):")
        for name, alive_c, total_c, ok, net, waf, cap, quar, ret in http_pool.source_breakdown():
            log.info(f"    {name:<11} {alive_c:>3}/{total_c:<3}  ok={ok:<6} net={net:<6} waf={waf:<5} cap={cap:<5} quar={quar:<4} spent={ret}")

    return completed


async def main():
    t0 = time.monotonic()

    cfg = load_scraper_config()
    install_shutdown_handler()

    listing_cache = get_current_state()
    seen = set(listing_cache.keys())
    log.info(f"cache: {len(seen)} ids")

    all_filters = build_filters_from_config(cfg)
    stats = {
        "parsed": 0,
        "captchas": 0,
        "skipped": 0,
        "saved": 0,
        "net_errors": 0,
        "waf_blocks": 0,
    }

    max_restarts = cfg.get("max_restarts", 5)
    cooldown = cfg.get("restart_cooldown", 60)
    completed = []

    for attempt in range(max_restarts + 1):
        if is_shutting_down():
            break

        if attempt > 0:
            stats["restarts"] = attempt
            log.info(
                f"\n=== RESTART {attempt}/{max_restarts}, cooldown {cooldown}s ==="
            )
            reset_restart()
            await jittered_delay(cooldown * 0.8, cooldown * 1.2)
            listing_cache.clear()
            seen.clear()
            listing_cache = get_current_state()
            seen = set(listing_cache.keys())
            log.info(f"cache: {len(seen)} ids")

        completed = await _run_session(all_filters, seen, stats, cfg, t0, listing_cache)

        if is_shutting_down():
            break
        if not is_restarting():
            break

    plan_size = stats.get("plan_size", 0)
    if not is_shutting_down() and not is_restarting() and len(completed) >= plan_size:
        log.info(f"план пройден целиком ({len(completed)}/{plan_size}), чекпоинт сброшен")
        clear_checkpoint("cian")

    elapsed_min = (time.monotonic() - t0) / 60
    rate = stats["parsed"] / elapsed_min if elapsed_min > 0 else 0
    total_planned = stats.get("total_planned", 0)
    parsed = stats["parsed"]
    saved = stats.get("saved", 0)

    w = 56
    log.info(f"\n{'=' * w}")
    log.info(f"  {'STOPPED' if is_shutting_down() else 'DONE'}  |  {elapsed_min:.1f}min  |  {rate:.0f}/min")
    log.info(f"{'=' * w}")

    p2_total = stats.get("p2_total", 0) or total_planned
    p2_pct = parsed * 100 // max(p2_total, 1)
    log.info(f"  cian total:  {total_planned:>8,}  (from filter counts)")
    log.info(f"  queued:      {p2_total:>8,}  (actual URLs for P2)")
    log.info(f"  parsed:      {parsed:>8,}  ({p2_pct}% of queued)")
    log.info(f"  from api:    {stats.get('api_parsed', 0):>8,}  (extracted from search json)")
    log.info(f"  saved:       {saved:>8,}")
    if stats.get("db_dropped"):
        log.info(f"  DB DROPPED:  {stats['db_dropped']:>8,}  (строки не прошли вставку)")
    log.info(f"  repriced:    {stats.get('price_changes', 0):>8,}")
    log.info(f"  similar:     {stats.get('similar_found', 0):>8,}")
    log.info(f"{'-' * w}")

    lst_cards = stats.get("listing_cards_total", 0)
    lst_skip_url = stats.get("listing_skip_url", 0)
    lst_skip_phrase = stats.get("listing_skip_phrase", 0)
    lst_skip = lst_skip_url + lst_skip_phrase
    lst_cached = stats.get("listing_cached", 0)
    if lst_cards:
        log.info(f"  listing cards:  {lst_cards:,} total")
        log.info(f"    cached:      {lst_cached:>7,}  (already in DB)")
        log.info(f"    skipped:     {lst_skip:>7,}  (url={lst_skip_url} phrase={lst_skip_phrase})")
    fl_incomplete = stats.get("filters_incomplete", 0)
    if fl_incomplete:
        log.info(f"    INCOMPLETE:  {fl_incomplete:>7,} filters, {stats.get('pages_failed', 0):,} pages lost (will retry next run)")
    empty_pages = stats.get("empty_pages", 0)
    if empty_pages:
        log.info(f"    empty pages: {empty_pages:>7,}  (200 без карточек)")
    log.info(f"{'-' * w}")

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

    net = stats.get("net_errors", 0)
    waf = stats.get("waf_blocks", 0)
    cap = stats.get("captchas", 0)
    vpn = stats.get("vpn_blocks", 0)
    bad_st = stats.get("bad_status", 0)
    final_failed = stats.get("final_failed", 0)
    total_errors = net + waf + cap + vpn + bad_st
    log.info(f"  net errors:  {total_errors:>7,}  (final_failed={final_failed})")
    if stats.get("pool_starved"):
        log.info(f"    pool empty:  {stats['pool_starved']:>7,}  (циклов без свободных слотов)")
    if net: log.info(f"    network:     {net:>7,}  " + " ".join(
        f"{k}={n}" for k, n in sorted(stats.get("net_kinds", {}).items(), key=lambda x: -x[1])))
    if waf: log.info(f"    waf:         {waf:>7,}")
    if cap: log.info(f"    captcha:     {cap:>7,}")
    if vpn: log.info(f"    vpn_block:   {vpn:>7,}")
    if bad_st: log.info(f"    bad_status:  {bad_st:>7,}")
    log.info(f"{'=' * w}")

    if SCRAPE_RUN_ID:
        insert_run_stats({
            "run_id": SCRAPE_RUN_ID,
            "minutes": round(elapsed_min, 1),
            "plan_offers": total_planned,
            "plan_filters": stats.get("filters_total", 0),
            "cards": lst_cards,
            "presence": stats.get("presence", 0),
            "parsed": parsed,
            "saved": saved,
            "repriced": stats.get("price_changes", 0),
            "empty_pages": empty_pages,
            "incomplete": fl_incomplete,
            "pages_lost": stats.get("pages_failed", 0),
            "captchas": cap,
            "net_errors": net,
            "waf_blocks": waf,
            "restarts": stats.get("restarts", 0),
            "pool_slots": stats.get("pool_slots", 0),
            "pool_alive": stats.get("pool_alive", 0),
        })

    return stats


if __name__ == "__main__":
    asyncio.run(main())
