"""
Скрапер квартир в новостройках с наш.дом.рф

Запуск:
    python -m scraper.domrf
"""

import asyncio
import json
import random
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright

from scraper.parsers_domrf import parse_domrf_offer

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# URL шаблоны для Москвы и МО
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


class DomrfScraper:
    def __init__(
        self,
        regions: list[str] = None,
        max_pages: int = 3,
        delay_range: tuple[float, float] = (8.0, 15.0),
        headless: bool = False,
    ):
        """
        regions:      список регионов
        max_pages:    сколько страниц пройти на регион
        delay_range:  задержка между запросами к квартирам
        headless:     без окна браузера
        """
        self.regions = regions or ["moscow"]
        self.max_pages = max_pages
        self.delay_range = delay_range
        self.headless = headless

        self.results: list[dict] = []
        self.seen_urls: set[str] = set()

    async def _wait_for_content(self, page, timeout: int = 30000):
        """Ждёт пока антибот-прелоадер пройдёт и загрузится контент"""
        await page.wait_for_timeout(3000)
        content = await page.content()
        if (
            "не робот" in content
            or "потяните" in content.lower()
            or "403" in await page.title()
        ):
            print("КАПЧА!")
            await asyncio.to_thread(input)
            await page.wait_for_timeout(3000)
        try:
            await page.wait_for_selector("text=Каталог", timeout=timeout)
        except Exception:
            await page.wait_for_timeout(15000)
        await page.wait_for_timeout(3000)

    async def _get_flat_urls(self, page) -> list[str]:
        """Собирает URL квартир с текущей страницы листинга"""
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

    async def _parse_flat(self, page, url: str) -> dict | None:
        """Переходит на страницу квартиры, парсит и возвращает dict"""
        await page.goto(url, timeout=30000)
        await self._wait_for_content(page)

        html = await page.content()
        data = parse_domrf_offer(html)
        data["url"] = url
        data["source"] = "domrf"
        data["is_new_building"] = True
        data["parsed_at"] = datetime.now().isoformat()
        return data

    def save_results(self, filename: str = "domrf_offers.json"):
        """Сохраняет результаты в JSON"""
        if not self.results:
            print("Нет данных для сохранения")
            return

        output_path = PROJECT_ROOT / "support_files" / filename
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        print(f"\nСохранено {len(self.results)} записей → {output_path}")

    async def _scrape_listing(self, page, region: str):
        """Последовательно проходит страницы листинга, собирает URL квартир."""
        all_urls = []
        for page_num in range(self.max_pages):
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

    async def _parse_flat_with_semaphore(self, semaphore, context, url):
        """Парсит одну квартиру, контролируемый семафором."""
        async with semaphore:
            page = await context.new_page()
            try:
                data = await self._parse_flat(page, url)
                if data:
                    self.results.append(data)
                    print(f"  Парсинг {url[-40:]} — ок")
            except Exception as e:
                print(f"  Ошибка: {url[-40:]} — {e}")
            finally:
                await page.close()
            await asyncio.sleep(random.uniform(*self.delay_range))

    async def scrape(self):
        PARALLEL_WORKERS = 4

        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=self.headless)
            context = await browser.new_context()
            semaphore = asyncio.Semaphore(PARALLEL_WORKERS)

            for region in self.regions:
                print(f"\n{'='*50}")
                print(f"РЕГИОН: {region}")
                print(f"{'='*50}")

                # 1. Собираем URL последовательно (одна вкладка)
                listing_page = await context.new_page()
                urls = await self._scrape_listing(listing_page, region)
                await listing_page.close()
                print(f"[{region}] Собрано {len(urls)} URL квартир")

                # 2. Парсим квартиры параллельно (семафор)
                tasks = [
                    self._parse_flat_with_semaphore(semaphore, context, url)
                    for url in urls
                ]
                await asyncio.gather(*tasks)

                print(f"[{region}] Завершён. Всего записей: {len(self.results)}")

            await context.close()
            await browser.close()

        self.save_results()


if __name__ == "__main__":
    scraper = DomrfScraper(regions=["moscow", "mo"], max_pages=100)
    asyncio.run(scraper.scrape())
