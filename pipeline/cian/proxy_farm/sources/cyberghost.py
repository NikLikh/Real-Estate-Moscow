# proxy_farm/sources/cyberghost.py
# CyberGhost серверы из статического JSON внутри CRX расширения
import asyncio
import json
import logging
from pathlib import Path

from pipeline.cian.proxy_farm.validator import check_connectivity

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
    sem = asyncio.Semaphore(10)
    seen_ips = set()

    async def check(country, dnsname):
        proxy = f"https://{dnsname}"
        async with sem:
            ip = await check_connectivity(proxy=proxy, timeout=8)
            if ip and ip not in seen_ips:
                seen_ips.add(ip)
                result.append((f"cg-{country}", proxy, ip))

    await asyncio.gather(*(check(c, d) for c, d in all_nodes))
    log.info(
        f"[HTTP] cyberghost: {len(result)}/{len(all_nodes)} nodes working ({len(seen_ips)} unique IPs)"
    )
    return result
