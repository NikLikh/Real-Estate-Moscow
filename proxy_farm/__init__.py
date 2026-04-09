# proxy_farm/__init__.py
import logging

from proxy_farm.pool import HttpPool, HttpSlot

log = logging.getLogger("re")


async def build_proxy_pool(cfg) -> HttpPool:
    from proxy_farm.sources import browsec, cyberghost, free_lists, monosans, oneclickvpn

    slots = []

    # CyberGhost: статический JSON из CRX
    try:
        from scraper.vpn_ext import download_extension
        download_extension("cyberghost")
    except Exception as e:
        log.debug(f"[HTTP] cyberghost download: {e}")
    cg = await cyberghost.discover(cfg)
    for label, proxy_url, ip in cg:
        slots.append(HttpSlot(proxy=proxy_url, label=label, ip=ip))
    if cg:
        log.info(f"[HTTP] cyberghost: {len(cg)} servers")

    # Browsec: headed browser для PAC
    try:
        brs = await browsec.discover(cfg)
        for label, proxy_url, ip in brs:
            slots.append(HttpSlot(proxy=proxy_url, label=label, ip=ip))
        if brs:
            log.info(f"[HTTP] browsec: {len(brs)} servers")
    except Exception as e:
        log.warning(f"[HTTP] browsec discovery failed (non-fatal): {e}")

    # 1clickVPN: API с S1+S2 валидацией
    # медленнее CG/Browsec, поэтому rate_limit ниже чтобы не забивать быстрые слоты
    slow_rate = cfg.get("slow_proxy_rate", 1.0)
    try:
        ocvpn = await oneclickvpn.discover(cfg)
        for label, proxy_url, ip in ocvpn:
            s = HttpSlot(proxy=proxy_url, label=label, ip=ip)
            s.rate_limit = slow_rate
            slots.append(s)
        if ocvpn:
            log.info(f"[HTTP] 1clickvpn: {len(ocvpn)} servers (S1+S2, rate={slow_rate})")
    except Exception as e:
        log.warning(f"[HTTP] 1clickvpn discovery failed: {e}")

    # monosans SOCKS5: JSON с S1+S2 валидацией
    try:
        mono = await monosans.discover(cfg)
        for label, proxy_url, ip in mono:
            s = HttpSlot(proxy=proxy_url, label=label, ip=ip)
            s.rate_limit = slow_rate
            slots.append(s)
        if mono:
            log.info(f"[HTTP] monosans: {len(mono)} SOCKS5 (S1+S2, rate={slow_rate})")
    except Exception as e:
        log.warning(f"[HTTP] monosans discovery failed: {e}")

    # бесплатные листы: только S1
    if cfg.get("free_proxy_discovery"):
        try:
            free = await free_lists.discover()
            for i, proxy_url in enumerate(free):
                proto = "socks5" if "socks5" in proxy_url else "http"
                slots.append(HttpSlot(proxy=proxy_url, label=f"free-{proto}-{i}"))
        except Exception as e:
            log.warning(f"[HTTP] free proxy discovery failed: {e}")

    budget = cfg.get("ip_budget", 9000)
    rate = cfg.get("http_rate_per_slot", 3.0)
    for s in slots:
        s.budget = budget

    log.info(f"[HTTP] pool ready: {len(slots)} slots, {rate} req/s per slot")
    max_conc = cfg.get("http_max_concurrent_per_proxy", 2)
    return HttpPool(slots, rate_limit=rate, max_concurrent_per_proxy=max_conc)
