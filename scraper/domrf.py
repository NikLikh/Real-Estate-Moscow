"""Скрапер наш.дом.рф"""

import asyncio
import logging
from datetime import datetime

from patchright.async_api import async_playwright

from db.repository import get_cached_urls, save_rows
from scraper.browser import (
    AdaptiveDelay,
    create_stealth_context,
    handle_captcha,
    humanize,
    jittered_delay,
    launch_stealth_browser,
)
from scraper.parsers_domrf import parse_domrf_offer
from scraper.runtime import (
    clear_checkpoint,
    install_shutdown_handler,
    is_shutting_down,
    load_checkpoint,
    save_checkpoint,
)

log = logging.getLogger("re")

REGIONS = {
    "moscow": (
        "https://xn--80az8a.xn--d1aqf.xn--p1ai/"
        "%D1%81%D0%B5%D1%80%D0%B2%D0%B8%D1%81%D1%8B/"
        "%D0%BA%D0%B0%D1%82%D0%B0%D0%BB%D0%BE%D0%B3-%D0%BA%D0%B2%D0%B0%D1%80%D1%82%D0%B8%D1%80/"
        "%D1%81%D0%BF%D0%B8%D1%81%D0%BE%D0%BA"
        "?flatStatus=free%2Cbooked&page={page}&limit=100&place=0-1"
    ),
    "mo": (
        "https://xn--80az8a.xn--d1aqf.xn--p1ai/"
        "%D1%81%D0%B5%D1%80%D0%B2%D0%B8%D1%81%D1%8B/"
        "%D0%BA%D0%B0%D1%82%D0%B0%D0%BB%D0%BE%D0%B3-%D0%BA%D0%B2%D0%B0%D1%80%D1%82%D0%B8%D1%80/"
        "%D1%81%D0%BF%D0%B8%D1%81%D0%BE%D0%BA"
        "?flatStatus=free%2Cbooked&page={page}&limit=100&place=50"
    ),
}

BASE_URL = "https://xn--80az8a.xn--d1aqf.xn--p1ai"

HEADLESS = True
NUM_CONTEXTS = 4
PAGES_PER_CONTEXT = 6


class DomrfScraper:
    def __init__(
        self,
        regions: list[str] = None,
        max_pages: int = 3,
        headless: bool = True,
    ):
        self.regions = regions or ["moscow"]
        self.max_pages = max_pages
        self.headless = headless

        self.results: list[dict] = []
        self.seen_urls: set[str] = set()
        self.stats = {"saved": 0, "errors": 0, "captchas": 0}

    def _load_cached_urls(self):
        self.seen_urls.update(get_cached_urls(["domrf"]))
        log.info(f"cache: {len(self.seen_urls)} urls in DB")

    def _save_to_db(self, rows: list[dict]) -> int:
        return save_rows(rows)

    async def _wait_for_content(self, page, timeout: int = 30000):
        await jittered_delay(2.0, 4.0)
        await humanize(page)

        if not await handle_captcha(page):
            self.stats["captchas"] += 1
            return False

        try:
            await page.wait_for_selector("text=Каталог", timeout=timeout)
        except Exception:
            await jittered_delay(10.0, 15.0)

        await jittered_delay(2.0, 4.0)
        return True

    async def _get_flat_urls(self, page) -> list[str]:
        links = await page.query_selector_all('a[href*="/квартира/"]')
        urls = []
        for link in links:
            href = await link.get_attribute("href")
            if not href:
                continue
            if not href.startswith("http"):
                href = BASE_URL + href
            if href not in self.seen_urls:
                urls.append(href)
                self.seen_urls.add(href)
        return urls

    async def _scrape_listing(self, page, region: str) -> list[str]:
        all_urls = []

        for page_num in range(self.max_pages):
            if is_shutting_down():
                log.info(f"  [{region}] stopping")
                break

            listing_url = REGIONS[region].format(page=page_num)
            log.info(f"  [{region}] listing {page_num + 1}: {listing_url}")

            try:
                await page.goto(listing_url, timeout=30000)
                ok = await self._wait_for_content(page)
                if not ok:
                    log.info(f"  [{region}] captcha on listing, skip page")
                    continue

                urls = await self._get_flat_urls(page)

                if not urls:
                    log.info(f"  [{region}] no flats, stop")
                    break

                all_urls.extend(urls)
                log.info(f"  [{region}] +{len(urls)} flats (total {len(all_urls)})")
            except Exception as e:
                log.error(f"  [{region}] listing error: {e}")

        return all_urls

    async def _parse_flat(self, page, url: str) -> dict | None:
        await page.goto(url, timeout=30000)
        ok = await self._wait_for_content(page)
        if not ok:
            return None

        html = await page.content()
        data = parse_domrf_offer(html)
        data["url"] = url
        data["source"] = "domrf"
        data["is_new_building"] = True
        data["parsed_at"] = datetime.now().isoformat()
        return data

    async def _worker(self, name: str, queue: asyncio.Queue, context):
        page = await context.new_page()
        delay = AdaptiveDelay()
        batch = []

        try:
            while not queue.empty() and not is_shutting_down():
                url = queue.get_nowait()

                try:
                    data = await self._parse_flat(page, url)
                    if data:
                        batch.append(data)
                        self.results.append(data)
                        delay.report_success()
                        log.info(f"  [{name}] ok {url[-40:]}")
                    else:
                        delay.report_captcha()
                except Exception as e:
                    self.stats["errors"] += 1
                    log.error(f"  [{name}] ERR {url[-40:]} - {e}")

                if len(batch) >= 10:
                    saved = self._save_to_db(batch)
                    self.stats["saved"] += saved
                    log.info(f"  [{name}] saved to DB: {saved}")
                    batch.clear()

                await delay.wait()

            if batch:
                saved = self._save_to_db(batch)
                self.stats["saved"] += saved
                log.info(f"  [{name}] saved to DB (remainder): {saved}")
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def scrape(self):
        install_shutdown_handler()
        self._load_cached_urls()

        checkpoint = load_checkpoint("domrf") or {"completed_regions": []}
        done_regions = set(checkpoint["completed_regions"])

        async with async_playwright() as pw:
            browser = await launch_stealth_browser(pw, headless=self.headless)

            try:
                for region in self.regions:
                    if region in done_regions:
                        log.info(f"\n[{region}] already done (checkpoint), skip")
                        continue

                    if is_shutting_down():
                        break

                    log.info(f"\n{'='*50}")
                    log.info(f"REGION: {region}")

                    context = await create_stealth_context(browser)
                    page = await context.new_page()
                    try:
                        urls = await self._scrape_listing(page, region)
                    finally:
                        await page.close()
                        await context.close()

                    log.info(f"[{region}] collected {len(urls)} flat URLs")

                    if not urls:
                        continue

                    queue = asyncio.Queue()
                    for url in urls:
                        queue.put_nowait(url)

                    total = NUM_CONTEXTS * PAGES_PER_CONTEXT
                    log.info(
                        f"[{region}] launching: {NUM_CONTEXTS} contexts "
                        f"x {PAGES_PER_CONTEXT} pages = {total} workers"
                    )

                    groups = []
                    for ctx_id in range(NUM_CONTEXTS):
                        ctx = await create_stealth_context(browser)
                        workers = [
                            asyncio.create_task(
                                self._worker(
                                    f"C{ctx_id+1}-W{j+1}", queue, ctx
                                )
                            )
                            for j in range(PAGES_PER_CONTEXT)
                        ]
                        groups.append((ctx, workers))

                    all_workers = [w for _, ws in groups for w in ws]
                    await asyncio.gather(*all_workers)

                    for ctx, _ in groups:
                        try:
                            await ctx.close()
                        except Exception:
                            pass

                    done_regions.add(region)
                    save_checkpoint("domrf", {"completed_regions": list(done_regions)})
                    log.info(f"[{region}] done. total records: {len(self.results)}")

            finally:
                await browser.close()

        if not is_shutting_down():
            clear_checkpoint("domrf")

        self._print_stats()

    def _print_stats(self):
        log.info(f"\n{'='*50}")
        log.info(f"TOTAL:")
        log.info(f"  parsed: {len(self.results)}")
        log.info(f"  saved to DB: {self.stats['saved']}")
        log.info(f"  errors: {self.stats['errors']}")
        log.info(f"  captchas: {self.stats['captchas']}")
        if is_shutting_down():
            log.info("stopped by signal. will resume from checkpoint.")


def main():
    scraper = DomrfScraper(
        regions=["moscow", "mo"], max_pages=100, headless=HEADLESS
    )
    asyncio.run(scraper.scrape())


if __name__ == "__main__":
    main()
