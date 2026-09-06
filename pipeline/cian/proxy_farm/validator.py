import asyncio
import logging
import time

from curl_cffi.requests import AsyncSession

from pipeline.cian.proxy_farm.detector import MAX_BODY_BYTES, headers, is_waf, is_captcha

log = logging.getLogger("re")

_API_CHECK_URL = "https://api.cian.ru/search-offers/v1/search-offers-mobile-site/"

_API_CHECK_BODY = {
    "jsonQuery": {
        "_type": "flatsale",
        "engine_version": {"type": "term", "value": 2},
        "region": {"type": "terms", "value": [1]},
        "room": {"type": "terms", "value": [1]},
        "page": {"type": "term", "value": 1},
    }
}


async def check_connectivity(proxy=None, timeout=5) -> str | None:
    try:
        async with AsyncSession(impersonate="chrome", proxy=proxy) as s:
            resp = await s.get("https://api.ipify.org", timeout=timeout)
            if len(resp.content) > MAX_BODY_BYTES:
                return None
            ip = resp.text.strip()
            if "." in ip:
                return ip
    except Exception:
        pass
    return None


async def check_cian_api(proxy=None, timeout=15, tries=3) -> bool:
    from pipeline.cian.api import api_headers

    h = api_headers(headers())
    try:
        async with AsyncSession(impersonate="chrome", proxy=proxy) as s:
            for _ in range(tries):
                resp = await s.post(
                    _API_CHECK_URL, json=_API_CHECK_BODY, headers=h, timeout=timeout
                )
                if len(resp.content) > MAX_BODY_BYTES:
                    return False
                if is_captcha(resp.text, str(resp.url)):
                    continue
                if resp.status_code == 429:
                    return True
                if is_waf(resp.text, resp.status_code):
                    return False
                data = resp.json().get("data") or {}
                return bool(data.get("offersSerialized"))
    except Exception:
        return False
    return False


async def check_cian_live(proxy=None, timeout=15, connect_timeout=0) -> float | None:
    from pipeline.cian.api import api_headers

    h = api_headers(headers())
    tmo = (connect_timeout, timeout - connect_timeout) if connect_timeout else timeout
    t0 = time.monotonic()
    try:
        async with AsyncSession(impersonate="chrome", proxy=proxy) as s:
            for attempt in range(2):
                resp = await s.post(
                    _API_CHECK_URL, json=_API_CHECK_BODY, headers=h, timeout=tmo
                )
                if len(resp.content) > MAX_BODY_BYTES:
                    return None
                if resp.status_code == 429:
                    return (time.monotonic() - t0) * 1000
                if is_captcha(resp.text, str(resp.url)):
                    continue
                if is_waf(resp.text, resp.status_code):
                    return None
                data = resp.json().get("data") or {}
                if data.get("offersSerialized"):
                    return (time.monotonic() - t0) * 1000
                return None
    except Exception:
        return None
    return None


async def validate_batch_live(
    proxies: list[tuple[str, str]], concurrency=250, timeout=15, connect_timeout=0
) -> list[tuple[str, str, float]]:
    sem = asyncio.Semaphore(concurrency)
    result = []

    async def check(label, proxy_url):
        async with sem:
            ms = await check_cian_live(proxy_url, timeout=timeout, connect_timeout=connect_timeout)
            if ms is not None:
                result.append((label, proxy_url, ms))

    await asyncio.gather(*(check(l, p) for l, p in proxies))
    return result


async def validate_proxy(proxy: str) -> tuple[str | None, bool]:
    ip = await check_connectivity(proxy=proxy)
    if not ip:
        return None, False
    cian_ok = await check_cian_api(proxy=proxy)
    return ip, cian_ok


async def validate_batch_s1s2(
    proxies: list[tuple[str, str]], concurrency=30, timeout=10
) -> list[tuple[str, str, str]]:
    sem = asyncio.Semaphore(concurrency)
    result = []

    async def check(label, proxy_url):
        async with sem:
            ip, cian_ok = await validate_proxy(proxy_url)
            if ip and cian_ok:
                result.append((label, proxy_url, ip))

    await asyncio.gather(*(check(l, p) for l, p in proxies))
    return result
