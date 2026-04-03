"""Проверка endpoints из scraper.yaml."""

import asyncio
import logging
import time

from patchright.async_api import async_playwright

from config.settings import load_scraper_config

log = logging.getLogger("re")


async def check_one(pw, ep_cfg):
    name = ep_cfg["name"]
    proxy_str = ep_cfg.get("proxy")
    proxy = {"server": proxy_str} if proxy_str else None

    # без --no-proxy-server direct endpoint подхватывает системный прокси
    browser = await pw.chromium.launch(headless=True, args=["--no-proxy-server"])
    try:
        ctx = await browser.new_context(proxy=proxy) if proxy else await browser.new_context()
        page = await ctx.new_page()

        # проверяем IP через httpbin
        ip_addr = "?"
        t0 = time.monotonic()
        try:
            await page.goto("https://httpbin.org/ip", timeout=15000)
            text = await page.inner_text("body")
            # httpbin отдает {"origin": "1.2.3.4"}
            ip_addr = text.split('"origin"')[1].split('"')[1].strip() if '"origin"' in text else text[:30]
        except Exception as e:
            ip_addr = f"ERR: {str(e)[:40]}"
        ip_dt = time.monotonic() - t0

        # проверяем cian.ru
        cian_ok = False
        t0 = time.monotonic()
        try:
            await page.goto("https://www.cian.ru/", timeout=20000, wait_until="domcontentloaded")
            title = await page.title()
            cian_ok = bool(title)  # любой непустой title = загрузилось
        except Exception:
            pass
        cian_dt = time.monotonic() - t0

        await page.close()
        await ctx.close()
    finally:
        await browser.close()

    return {
        "name": name,
        "ip": ip_addr,
        "cian_ok": cian_ok,
        "latency": ip_dt + cian_dt,
    }


async def run():
    cfg = load_scraper_config()
    endpoints = cfg.get("endpoints", [{"name": "direct", "proxy": None}])

    print(f"\nchecking {len(endpoints)} endpoints...\n")
    print(f"{'endpoint':<12} | {'IP':<18} | {'cian':<6} | latency")
    print("-" * 55)

    async with async_playwright() as pw:
        for ep in endpoints:
            result = await check_one(pw, ep)
            cian_str = "ok" if result["cian_ok"] else "FAIL"
            print(
                f"{result['name']:<12} | {result['ip']:<18} | {cian_str:<6} | {result['latency']:.1f}s"
            )

    print()


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
