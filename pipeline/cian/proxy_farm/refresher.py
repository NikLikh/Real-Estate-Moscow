# proxy_farm/refresher.py
# фоновое обновление прокси-пула из всех источников
import asyncio
import logging
import time

from pipeline.cian.proxy_farm.pool import HttpPool, HttpSlot
from pipeline.cian.proxy_farm.validator import check_connectivity, check_cian

log = logging.getLogger("re")


def _diff_and_update(pool: HttpPool, discovered: list[tuple[str, str, str]], source: str):
    existing = pool.slot_labels()
    added = 0
    for label, proxy_url, ip in discovered:
        if label not in existing:
            pool.add_slot(HttpSlot(proxy=proxy_url, label=label, ip=ip))
            added += 1
        else:
            # слот уже есть, сбрасываем cooldown если прокси жив
            pool.reset_cooldown(label)
    if added:
        log.debug(f"[REFRESH] {source}: +{added} new, total {pool.slot_count} slots")
    return added


async def _check_existing_slots(pool: HttpPool, source_prefix: str):
    labels = [l for l in pool.slot_labels() if l.startswith(source_prefix)]
    if not labels:
        return

    sem = asyncio.Semaphore(5)

    async def check_one(label):
        slot = pool.get_slot(label)
        if not slot:
            return
        async with sem:
            ip = await check_connectivity(proxy=slot.proxy, timeout=5)
            if not ip:
                pool.remove_slot(label)
                log.debug(f"[REFRESH] removed dead slot {label}")
                return
            cian_ok = await check_cian(proxy=slot.proxy, timeout=10)
            if not cian_ok:
                slot.cooldown_until = time.monotonic() + 300
            else:
                pool.reset_cooldown(label)

    await asyncio.gather(*[check_one(l) for l in labels])


async def _light_cycle(pool: HttpPool, cfg: dict):
    from pipeline.cian.proxy_farm.sources import cyberghost, free_lists, monosans, oneclickvpn

    # 1clickVPN
    try:
        servers = await oneclickvpn.discover(cfg)
        _diff_and_update(pool, servers, "1clickvpn")
    except Exception as e:
        log.debug(f"[REFRESH] 1clickvpn error: {e}")

    # monosans SOCKS5
    try:
        servers = await monosans.discover(cfg)
        _diff_and_update(pool, servers, "monosans")
    except Exception as e:
        log.debug(f"[REFRESH] monosans error: {e}")

    # CyberGhost
    try:
        servers = await cyberghost.discover(cfg)
        _diff_and_update(pool, servers, "cyberghost")
    except Exception as e:
        log.debug(f"[REFRESH] cyberghost error: {e}")

    # бесплатные листы
    if cfg.get("free_proxy_discovery"):
        try:
            free = await free_lists.discover()
            for i, proxy_url in enumerate(free):
                proto = "socks5" if "socks5" in proxy_url else "http"
                label = f"free-{proto}-{i}"
                if label not in pool.slot_labels():
                    pool.add_slot(HttpSlot(proxy=proxy_url, label=label))
        except Exception as e:
            log.debug(f"[REFRESH] free_lists error: {e}")

    # чистим мёртвые слоты от бесплатных источников
    await _check_existing_slots(pool, "mono-")
    await _check_existing_slots(pool, "1click-")


async def _heavy_cycle(pool: HttpPool, cfg: dict):
    from pipeline.cian.proxy_farm.sources import browsec

    try:
        servers = await browsec.discover(cfg)
        _diff_and_update(pool, servers, "browsec")
        # удаляем мёртвые browsec слоты
        await _check_existing_slots(pool, "vpn-")
    except Exception as e:
        log.debug(f"[REFRESH] browsec error: {e}")


async def run_refresher(pool: HttpPool, cfg: dict):
    light_interval = cfg.get("proxy_refresh_interval", 300)
    heavy_interval = cfg.get("browsec_refresh_interval", 600)

    async def light_loop():
        while True:
            await asyncio.sleep(light_interval)
            try:
                log.debug(f"[REFRESH] light cycle, pool: {pool.alive}/{pool.slot_count} alive")
                await _light_cycle(pool, cfg)
                log.debug(f"[REFRESH] light done, pool: {pool.alive}/{pool.slot_count} alive")
            except Exception as e:
                log.warning(f"[REFRESH] light cycle error: {e}")

    async def heavy_loop():
        while True:
            await asyncio.sleep(heavy_interval)
            try:
                log.debug(f"[REFRESH] heavy cycle (browsec), pool: {pool.alive}/{pool.slot_count}")
                await _heavy_cycle(pool, cfg)
                log.debug(f"[REFRESH] heavy done, pool: {pool.alive}/{pool.slot_count} alive")
            except Exception as e:
                log.warning(f"[REFRESH] heavy cycle error: {e}")

    await asyncio.gather(light_loop(), heavy_loop())
