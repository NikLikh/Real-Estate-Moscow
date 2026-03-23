"""
Скрапер объявлений cian

Запуск:
    python -m scraper.cian
"""

import asyncio
import json
import random
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright

from scraper.parsers import parse_offer_page

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class CianScraper:
    def __init__(
        self,
        base_url: str,
        max_pages: int = 3,
        delay_range: tuple[float, float] = (3.0, 6.0),
        headless: bool = False,
    ):
        """
        base_url:     URL страницы списка
        max_pages:    сколько страниц списка пройти
        delay_range:  диапазон случайной задержки между запросами
        headless
        """
        self.base_url = base_url
        self.max_pages = max_pages
        self.delay_range = delay_range
        self.headless = headless

        self.results: list[dict] = []
        self.seen_urls: set[str] = set()

    async def _get_offer_urls(self, page) -> list[str]:
        """Собирает URL объявлений с текущей страницы списка"""
        try:
            await page.wait_for_selector(
                'article[data-name="CardComponent"]', timeout=15000
            )
        except Exception:
            print("  Карточки не найдены (возможно CAPTCHA)")
            return []

        cards = await page.query_selector_all('article[data-name="CardComponent"]')
        urls = []
        for card in cards:
            link = await card.query_selector('div[data-name="LinkArea"] a')
            if link:
                href = await link.get_attribute("href")
                if href and href not in self.seen_urls:
                    urls.append(href)
                    self.seen_urls.add(href)
        return urls

    async def _parse_offer(self, page, url: str) -> list[dict]:
        """Переходит на страницу объявления, парсит и возвращает dict"""
        await page.goto(url, timeout=30000)
        await page.wait_for_timeout(3000)

        html = await page.content()
        data, price_history = parse_offer_page(html)
        data["url"] = url
        data["source"] = "cian"
        data["parsed_at"] = datetime.now().isoformat()

        # Текущее объявление — основная запись
        rows = [data]

        # Исторические цены — копии с подменой price и publication_date
        # Пропускаем если цена совпадает с текущей (дубликат)
        current_price = data.get("price")
        for entry in price_history:
            if entry["price"] == current_price:
                continue
            historical = data.copy()
            historical["price"] = entry["price"]
            historical["publication_date"] = entry["date"]
            historical["source"] = "cian_history"
            rows.append(historical)

        return rows

    def _next_page_url(self, page_num: int) -> str:
        """Формирует URL следующей страницы списка: &p=2, &p=3"""
        sep = "&" if "?" in self.base_url else "?"
        return f"{self.base_url}{sep}p={page_num}"

    def save_results(self, filename: str = "cian_offers.json"):
        """Сохраняет результаты в JSON."""
        if not self.results:
            print("Нет данных для сохранения")
            return

        output_path = PROJECT_ROOT / "support_files" / filename
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        print(f"\nСохранено {len(self.results)} записей → {output_path}")

    async def scrape(self):
        """Главный метод: запускает браузер, проходит по страницам, собирает данные и сохраняет"""

        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=self.headless)
            page = await browser.new_page()

            for page_num in range(1, self.max_pages + 1):

                next_url = self._next_page_url(page_num)
                print(f"\nПереход на страницу {page_num}: {next_url}")
                await page.goto(next_url, timeout=30000)
                urls = await self._get_offer_urls(page)

                if not urls:
                    print("  Нет объявлений или CAPTCHA. Прерывание")
                    break
                print(f"  Найдено {len(urls)} объявлений на странице {page_num}")

                for url in urls:

                    try:
                        rows = await self._parse_offer(page, url)

                        if rows:
                            self.results.extend(rows)
                            print(
                                f"Парсинг {url} - {len(rows)} записей (1 текущая + {len(rows)-1} история)"
                            )

                        else:
                            print(f"Парсинг {url} - нет данных")

                    except Exception as e:
                        print(f"Ошибка при парсинге {url}: {e}")

                    await asyncio.sleep(random.uniform(*self.delay_range))

                print(f"Страниц обработано: {page_num / self.max_pages * 100:.1f}%")

            await browser.close()

        self.save_results()


if __name__ == "__main__":
    BASE_URL = (
        "https://www.cian.ru/cat.php?"
        "deal_type=sale&engine_version=2&offer_type=flat&region=-1"
    )

    scraper = CianScraper(base_url=BASE_URL, max_pages=1)
    asyncio.run(scraper.scrape())
