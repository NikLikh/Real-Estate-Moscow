# proxy_farm/sources/free_lists.py
# 20+ бесплатных proxy-листов (GitHub raw, API)
import asyncio
import logging
import random
import re

from curl_cffi.requests import AsyncSession

from pipeline.cian.proxy_farm.validator import check_connectivity

log = logging.getLogger("re")

PROXY_SOURCES = [
    ("socks5", "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt"),
    ("socks4", "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt"),
    ("http", "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt"),
    ("socks5", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt"),
    ("socks4", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt"),
    ("http", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"),
    ("socks5", "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt"),
    ("socks5", "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt"),
    ("socks4", "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks4.txt"),
    ("http", "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt"),
    ("socks5", "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt"),
    ("socks4", "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt"),
    ("http", "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt"),
    ("socks5", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt"),
    ("socks4", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks4/data.txt"),
    ("http", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"),
    ("socks5", "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=5000&country=all"),
    ("socks4", "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=5000&country=all"),
    ("http", "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all"),
    ("http", "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=ru"),
    ("socks5", "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=5000&country=ru"),
    ("http", "https://proxylist.geonode.com/api/proxy-list?country=RU&limit=500&sort_by=lastChecked&sort_type=desc&protocols=http"),
    ("socks5", "https://spys.me/socks.txt"),
    ("http", "https://spys.me/proxy.txt"),
]

_ADDR_RE = re.compile(r"^(\d{1,3}\.){3}\d{1,3}:\d{2,5}$")


async def _fetch_all_candidates() -> dict[str, str]:
    candidates = {}

    async with AsyncSession(impersonate="chrome") as s:
        fetch_tasks = []
        for default_proto, url in PROXY_SOURCES:

            async def fetch_one(default_proto=default_proto, url=url):
                try:
                    resp = await s.get(url, timeout=8)
                    for line in resp.text.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        proto = default_proto
                        for pfx in ("socks5://", "socks4://", "http://", "https://"):
                            if line.startswith(pfx):
                                proto = pfx.rstrip(":/")
                                line = line[len(pfx):]
                                break
                        addr = line.split()[0]
                        if _ADDR_RE.match(addr) and addr not in candidates:
                            candidates[addr] = proto
                except Exception:
                    pass

            fetch_tasks.append(fetch_one())

        try:
            await asyncio.wait_for(asyncio.gather(*fetch_tasks), timeout=15)
        except asyncio.TimeoutError:
            pass

    return candidates


async def _validate_batch(items, timeout=4, validation_timeout=30) -> list[str]:
    sem = asyncio.Semaphore(150)
    working = []

    async def check(addr, proto):
        proxy_url = f"{proto}://{addr}"
        async with sem:
            ip = await check_connectivity(proxy=proxy_url, timeout=timeout)
            if ip:
                working.append(proxy_url)

    try:
        await asyncio.wait_for(
            asyncio.gather(*(check(a, p) for a, p in items)),
            timeout=validation_timeout,
        )
    except asyncio.TimeoutError:
        pass

    return working


async def discover(
    timeout=4,
    batch_size=2000,
    validation_timeout=30,
    min_target=10,
    max_rounds=5,
) -> list[str]:
    candidates = await _fetch_all_candidates()

    log.info(
        f"[HTTP] proxy sources: {len(candidates)} unique candidates from {len(PROXY_SOURCES)} sources"
    )
    if not candidates:
        return []

    all_items = list(candidates.items())
    random.shuffle(all_items)

    working = []
    offset = 0

    for round_n in range(1, max_rounds + 1):
        batch = all_items[offset : offset + batch_size]
        if not batch:
            break
        offset += batch_size

        log.info(f"[HTTP] round {round_n}: validating {len(batch)} proxies...")
        found = await _validate_batch(
            batch, timeout=timeout, validation_timeout=validation_timeout
        )
        working.extend(found)
        log.info(f"[HTTP] round {round_n}: +{len(found)}, total {len(working)} working")

        if len(working) >= min_target:
            break

    log.info(
        f"[HTTP] free proxies: {len(working)}/{min(offset, len(all_items))} working"
    )
    return working
