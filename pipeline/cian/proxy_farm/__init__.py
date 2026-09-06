import asyncio
import logging
import time

from pipeline.cian.proxy_farm.pool import HttpPool, HttpSlot

log = logging.getLogger("re")


async def _seed_public(cfg):
    from pipeline.cian.proxy_farm.sources import public_lists
    from pipeline.cian.proxy_farm.validator import check_cian_live

    cached = public_lists.load_cache()
    if not cached:
        return []
    timeout = cfg.get("proxy_validate_timeout", 20)
    connect_timeout = cfg.get("proxy_connect_timeout", 12)
    sem = asyncio.Semaphore(cfg.get("validation_concurrency", 50))
    alive = []

    async def check(proxy):
        async with sem:
            ms = await check_cian_live(proxy=proxy, timeout=timeout, connect_timeout=connect_timeout)
            if ms is not None:
                alive.append(proxy)

    t0 = time.monotonic()
    await asyncio.gather(*(check(p) for p in cached))
    log.info(f"[HTTP] кэш прокси: {len(alive)}/{len(cached)} живых за {time.monotonic() - t0:.0f}s")
    return alive


async def build_proxy_pool(cfg) -> HttpPool:
    from pipeline.cian.proxy_farm.sources import browsec, cyberghost, free_lists, oneclickvpn

    slots = []

    if cfg.get("use_direct_slot", True):
        slots.append(HttpSlot(proxy=None, label="direct", source="direct"))

    try:
        from pipeline.cian.vpn_ext import download_extension
        download_extension("cyberghost")
    except Exception as e:
        log.debug(f"[HTTP] cyberghost download: {e}")
    try:
        cg = await asyncio.wait_for(cyberghost.discover(cfg), cfg.get("vpn_discovery_timeout", 90))
    except Exception:
        cg = []
    for label, proxy_url, ip in cg:
        slots.append(HttpSlot(proxy=proxy_url, label=label, ip=ip, source="cyberghost"))
    if cg:
        log.info(f"[HTTP] cyberghost: {len(cg)} servers")

    try:
        brs = await asyncio.wait_for(browsec.discover(cfg), cfg.get("vpn_discovery_timeout", 90))
        for label, proxy_url, ip in brs:
            slots.append(HttpSlot(proxy=proxy_url, label=label, ip=ip, source="browsec"))
        if brs:
            log.info(f"[HTTP] browsec: {len(brs)} servers")
    except Exception as e:
        log.warning(f"[HTTP] browsec discovery failed (non-fatal): {e}")

    slow_rate = cfg.get("slow_proxy_rate", 1.0)
    if cfg.get("enable_oneclickvpn", False):
        try:
            ocvpn = await oneclickvpn.discover(cfg)
            for label, proxy_url, ip in ocvpn:
                s = HttpSlot(proxy=proxy_url, label=label, ip=ip, source="1clickvpn")
                s.rate_limit = slow_rate
                slots.append(s)
            if ocvpn:
                log.info(f"[HTTP] 1clickvpn: {len(ocvpn)} servers (S1+S2, rate={slow_rate})")
        except Exception as e:
            log.warning(f"[HTTP] 1clickvpn discovery failed: {e}")

    for proxy_url in await _seed_public(cfg):
        slots.append(HttpSlot(proxy=proxy_url, label=f"pub-{proxy_url.split('//')[1]}", source="public"))

    if cfg.get("free_proxy_discovery"):
        try:
            free = await free_lists.discover()
            for proxy_url in free:
                proto = "socks5" if "socks5" in proxy_url else "http"
                label = f"free-{proto}-{proxy_url.split('//')[-1]}"
                slots.append(HttpSlot(proxy=proxy_url, label=label, source="free"))
        except Exception as e:
            log.warning(f"[HTTP] free proxy discovery failed: {e}")

    budget = cfg.get("ip_budget", 165)
    rate = cfg.get("http_rate_per_slot", 3.0)
    max_conc = cfg.get("http_max_concurrent_per_proxy", 2)
    burst = cfg.get("slot_burst", 3)
    window = cfg.get("slot_window", 30.0)
    pool = HttpPool(slots, rate_limit=rate, max_concurrent_per_proxy=max_conc,
                    burst=burst, window=window,
                    quarantine_cooldown=cfg.get("net_error_quarantine_cooldown", 1800),
                    waf_limit=cfg.get("waf_retire_limit", 8), budget=budget)
    n = pool.slot_count
    if len(slots) > n:
        log.info(f"[HTTP] схлопнуто {len(slots) - n} слотов с общим хостом")
    log.info(
        f"[HTTP] pool seed: {n} slots, {rate} req/s x {max_conc} inflight, бюджет {budget} req/IP"
    )
    return pool


async def wait_for_pool(pool, need, limit):
    t0 = time.monotonic()
    while time.monotonic() - t0 < limit and pool.alive < need:
        await asyncio.sleep(2)
    return pool.alive >= need
