# proxy_farm/sources/browsec.py
# Browsec серверы из PAC данных через headed browser
import asyncio
import logging

from proxy_farm.validator import check_connectivity

log = logging.getLogger("re")


async def discover(cfg) -> list[tuple[str, str, str]]:
    vpn_cfg = cfg.get("vpn_extensions", [])
    browsec_cfg = next((v for v in vpn_cfg if v.get("extension") == "browsec"), None)
    if not browsec_cfg:
        return []

    servers_to_try = browsec_cfg.get("servers", [])
    if not servers_to_try:
        return []

    log.info("[HTTP] extracting Browsec proxy servers...")

    try:
        from patchright.async_api import async_playwright
        from scraper.vpn_ext import launch_vpn_context
    except ImportError:
        return []

    result = []
    try:
        async with async_playwright() as pw:
            ctx, bg = await launch_vpn_context(
                pw, "browsec", servers_to_try[0], headless=False
            )
            try:
                pac_data = await bg.evaluate("""async () => {
                    const items = await new Promise(r => chrome.storage.local.get('lowLevelPac', r));
                    return items['lowLevelPac'];
                }""")
            finally:
                await ctx.close()

        if not pac_data or "countries" not in pac_data:
            log.warning("[HTTP] browsec: no PAC data")
            return []

        # собираем серверы из всех стран
        all_servers = []
        for country, servers in pac_data["countries"].items():
            for raw in servers:
                addr = raw.replace("HTTPS ", "").replace("HTTP ", "")
                all_servers.append((country, addr))

        log.info(
            f"[HTTP] browsec: {len(all_servers)} servers from {len(pac_data['countries'])} countries"
        )

        sem = asyncio.Semaphore(10)

        async def check(country, addr):
            proxy = f"https://{addr}"
            async with sem:
                ip = await check_connectivity(proxy=proxy, timeout=8)
                if ip:
                    result.append((f"vpn-{country}", proxy, ip))

        await asyncio.gather(*(check(c, a) for c, a in all_servers))
        log.info(f"[HTTP] browsec: {len(result)}/{len(all_servers)} servers working")

    except Exception as e:
        log.warning(f"[HTTP] browsec discovery failed: {e}")

    return result
