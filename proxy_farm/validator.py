# proxy_farm/validator.py
# 2-этапная валидация прокси: S1 connectivity + S2 cian WAF check
import asyncio
import logging

from curl_cffi.requests import AsyncSession

from proxy_farm.detector import headers, is_waf, is_captcha, is_vpn_block

log = logging.getLogger("re")

# минимальный размер нормальной страницы cian (~2.6MB)
# captcha/waf-страницы обычно ~40K
_CIAN_MIN_SIZE = 100_000


async def check_connectivity(proxy=None, timeout=5) -> str | None:
    try:
        async with AsyncSession(impersonate="chrome", proxy=proxy) as s:
            resp = await s.get("https://api.ipify.org", timeout=timeout)
            ip = resp.text.strip()
            if "." in ip:
                return ip
    except Exception:
        pass
    return None


async def check_cian(proxy=None, timeout=10) -> bool:
    try:
        async with AsyncSession(impersonate="chrome", proxy=proxy) as s:
            resp = await s.get(
                "https://www.cian.ru/", headers=headers(), timeout=timeout
            )
            html = resp.text
            if is_waf(html, resp.status_code):
                return False
            if is_captcha(html):
                return False
            if is_vpn_block(html):
                return False
            if len(html) < _CIAN_MIN_SIZE:
                return False
            return True
    except Exception:
        return False


async def validate_proxy(proxy: str) -> tuple[str | None, bool]:
    ip = await check_connectivity(proxy=proxy)
    if not ip:
        return None, False
    cian_ok = await check_cian(proxy=proxy)
    return ip, cian_ok


async def validate_batch_s1(items, timeout=4, validation_timeout=30) -> list[str]:
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


async def validate_batch_s1s2(
    proxies: list[tuple[str, str]], concurrency=10, timeout=10
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
