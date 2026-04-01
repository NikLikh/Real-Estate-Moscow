"""
Скрапер квартир в новостройках с наш.дом.рф

Запуск:
    python -m scraper.domrf
"""

import asyncio
import random
from datetime import datetime

from camoufox.async_api import AsyncCamoufox

from db.loader import get_cached_urls, save_rows
from scraper.parsers_domrf import parse_domrf_offer
from scraper.utils import (
    clear_checkpoint,
    install_shutdown_handler,
    is_shutting_down,
    load_checkpoint,
    managed_page,
    save_checkpoint,
)

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

PARALLEL_WORKERS = 2


class DomrfScraper:
    def __init__(
        self,
        regions: list[str] = None,
        max_pages: int = 3,
        delay_range: tuple[float, float] = (8.0, 15.0),
        headless: bool = False,
    ):
        self.regions = regions or ["moscow"]
        self.max_pages = max_pages
        self.delay_range = delay_range
        self.headless = headless

        self.results: list[dict] = []
        self.seen_urls: set[str] = set()
        self.stats = {"saved": 0, "errors": 0}

    def _load_cached_urls(self):
        self.seen_urls.update(get_cached_urls(["domrf"]))
        print(f"Кэш: {len(self.seen_urls)} URL уже в БД")

    def _save_to_db(self, rows: list[dict]) -> int:
        return save_rows(rows)

    async def _wait_for_content(self, page, timeout: int = 30000):
        """Ждем пока антибот-прелоадер пройдет и загрузится контент."""
        await page.wait_for_timeout(3000)
        content = await page.content()

        if (
            "не робот" in content
            or "потяните" in content.lower()
            or "403" in await page.title()
        ):
            print("  КАПЧА! Решите в браузере и нажмите Enter...")
            await asyncio.to_thread(input)
            await page.wait_for_timeout(3000)

        try:
            await page.wait_for_selector("text=Каталог", timeout=timeout)
        except Exception:
            await page.wait_for_timeout(15000)

        await page.wait_for_timeout(3000)

    async def _get_flat_urls(self, page) -> list[str]:
        """Собирает URL квартир со страницы листинга."""
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
        """Проходит страницы листинга, собирает все URL квартир."""
        all_urls = []

        for page_num in range(self.max_pages):
            if is_shutting_down():
                print(f"  [{region}] Остановка по сигналу")
                break

            listing_url = REGIONS[region].format(page=page_num)
            print(f"  [{region}] Листинг {page_num + 1}: {listing_url}")

            try:
                await page.goto(listing_url, timeout=30000)
                await self._wait_for_content(page)
                urls = await self._get_flat_urls(page)

                if not urls:
                    print(f"  [{region}] Нет квартир, стоп.")
                    break

                all_urls.extend(urls)
                print(f"  [{region}] +{len(urls)} квартир (всего {len(all_urls)})")
            except Exception as e:
                print(f"  [{region}] Ошибка листинга: {e}")

        return all_urls

    async def _parse_flat(self, page, url: str) -> dict | None:
        """Грузит страницу квартиры и парсит."""
        await page.goto(url, timeout=30000)
        await self._wait_for_content(page)

        html = await page.content()
        data = parse_domrf_offer(html)
        data["url"] = url
        data["source"] = "domrf"
        data["is_new_building"] = True
        data["parsed_at"] = datetime.now().isoformat()
        return data

    async def _worker(self, name: str, queue: asyncio.Queue, context):
        """Один воркер - одна страница браузера. Берет задачи из очереди."""
        async with managed_page(context) as page:
            batch = []

            while not queue.empty() and not is_shutting_down():
                url = queue.get_nowait()

                try:
                    data = await self._parse_flat(page, url)
                    if data:
                        batch.append(data)
                        self.results.append(data)
                        print(f"  [{name}] ok {url[-40:]}")
                except Exception as e:
                    self.stats["errors"] += 1
                    print(f"  [{name}] ERR {url[-40:]} - {e}")

                # flush каждые 10 шт
                if len(batch) >= 10:
                    saved = self._save_to_db(batch)
                    self.stats["saved"] += saved
                    print(f"  [{name}] Сохранено в БД: {saved}")
                    batch.clear()

                await asyncio.sleep(random.uniform(*self.delay_range))

            if batch:
                saved = self._save_to_db(batch)
                self.stats["saved"] += saved
                print(f"  [{name}] Сохранено в БД (остаток): {saved}")

    async def scrape(self):
        install_shutdown_handler()
        self._load_cached_urls()

        checkpoint = load_checkpoint("domrf") or {"completed_regions": []}
        done_regions = set(checkpoint["completed_regions"])

        async with AsyncCamoufox(
            headless=self.headless, locale="ru-RU", humanize=True
        ) as browser:
            context = await browser.new_context()

            # не грузим медиа/шрифты/стили - быстрее
            BLOCKED_TYPES = {"media", "font", "stylesheet"}
            await context.route(
                "**/*",
                lambda route: (
                    route.abort()
                    if route.request.resource_type in BLOCKED_TYPES
                    else route.continue_()
                ),
            )

            try:
                for region in self.regions:
                    if region in done_regions:
                        print(f"\n[{region}] Уже обработан (чекпоинт), пропускаю")
                        continue

                    if is_shutting_down():
                        break

                    print(f"\n{'='*50}")
                    print(f"РЕГИОН: {region}")
                    print(f"{'='*50}")

                    # Собираем URL квартир с листинга
                    async with managed_page(context) as listing_page:
                        urls = await self._scrape_listing(listing_page, region)
                    print(f"[{region}] Собрано {len(urls)} URL квартир")

                    if not urls:
                        continue

                    # раздаем URL воркерам
                    queue = asyncio.Queue()
                    for url in urls:
                        queue.put_nowait(url)

                    workers = [
                        asyncio.create_task(self._worker(f"W{i+1}", queue, context))
                        for i in range(PARALLEL_WORKERS)
                    ]
                    await asyncio.gather(*workers)

                    # регион обработан
                    done_regions.add(region)
                    save_checkpoint("domrf", {"completed_regions": list(done_regions)})
                    print(f"[{region}] Готово. Всего записей: {len(self.results)}")

            finally:
                await context.close()

        # все ок - чекпоинт больше не нужен
        if not is_shutting_down():
            clear_checkpoint("domrf")

        self._print_stats()

    def _print_stats(self):
        print(f"\n{'='*50}")
        print(f"ИТОГО:")
        print(f"  Спарсено: {len(self.results)}")
        print(f"  Сохранено в БД: {self.stats['saved']}")
        print(f"  Ошибок: {self.stats['errors']}")
        if is_shutting_down():
            print("Остановлено по сигналу. При перезапуске продолжит с чекпоинта.")
        print(f"{'='*50}")


if __name__ == "__main__":
    scraper = DomrfScraper(regions=["moscow", "mo"], max_pages=100)
    asyncio.run(scraper.scrape())
