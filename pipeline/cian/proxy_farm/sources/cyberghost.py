import asyncio
import json
import logging
from pathlib import Path

from pipeline.cian.proxy_farm.validator import check_cian_api, check_connectivity

log = logging.getLogger("re")

_EXT_DIR = Path(__file__).resolve().parent.parent.parent / "extensions"


async def discover(cfg=None) -> list[tuple[str, str, str]]:
    server_list = _EXT_DIR / "cyberghost" / "assets" / "server_list.json"
    if not server_list.exists():
        return []

    with open(server_list) as f:
        locations = json.load(f)

    all_nodes = []
    for loc in locations:
        for node in loc.get("nodes", []):
            if node.get("dnsname"):
                all_nodes.append((loc["name"], node["dnsname"]))

    if not all_nodes:
        return []

    log.info(f"[HTTP] cyberghost: validating {len(all_nodes)} nodes...")
    result = []
    sem = asyncio.Semaphore((cfg or {}).get("validation_concurrency", 30))
    seen_ips = set()

    api_check = (cfg or {}).get("cian_validation", True)

    async def check(country, dnsname):
        proxy = f"https://{dnsname}"
        async with sem:
            ip = await check_connectivity(proxy=proxy, timeout=8)
            if not ip or ip in seen_ips:
                return
            if api_check and not await check_cian_api(proxy=proxy):
                return
            seen_ips.add(ip)
            result.append((f"cg-{country}-{dnsname}", proxy, ip))

    await asyncio.gather(*(check(c, d) for c, d in all_nodes))
    log.info(
        f"[HTTP] cyberghost: {len(result)}/{len(all_nodes)} nodes working ({len(seen_ips)} unique IPs)"
    )
    return result
