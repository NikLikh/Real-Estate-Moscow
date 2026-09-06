import logging

from curl_cffi.requests import AsyncSession

from pipeline.cian.proxy_farm.validator import validate_batch_s1s2

log = logging.getLogger("re")

_URL = "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies.json"


async def discover(cfg=None) -> list[tuple[str, str, str]]:
    try:
        async with AsyncSession(impersonate="chrome") as s:
            resp = await s.get(_URL, timeout=10)
            data = resp.json()
    except Exception as e:
        log.warning(f"[HTTP] monosans: fetch error: {e}")
        return []

    if not isinstance(data, list):
        log.warning("[HTTP] monosans: unexpected JSON format")
        return []

    candidates = []
    idx = 0
    for item in data:
        if item.get("protocol") != "socks5":
            continue
        host = item.get("host")
        port = item.get("port")
        if not host or not port:
            continue

        geo = item.get("geolocation", {})
        country = geo.get("country", {})
        cc = country.get("iso_code", "xx") if isinstance(country, dict) else "xx"
        cc = cc.lower()

        proxy_url = f"socks5://{host}:{port}"
        label = f"mono-{cc}-{idx}"
        candidates.append((label, proxy_url))
        idx += 1

    if not candidates:
        return []

    log.info(f"[HTTP] monosans: {len(candidates)} SOCKS5 candidates, validating S1+S2...")

    result = await validate_batch_s1s2(
        candidates, concurrency=(cfg or {}).get("validation_concurrency", 30), timeout=10
    )
    log.info(f"[HTTP] monosans: {len(result)}/{len(candidates)} passed S1+S2")
    return result
