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

import psycopg2
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from psycopg2.extras import Json

from db.loader import COLUMNS, DB_CONFIG, INSERT_SQL, _build_row
from scraper.parsers import parse_offer_page

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class CianScraper:
    def __init__(
        self,
        base_url: str,
        max_pages: int = 3,
        delay_range: tuple[float, float] = (5.0, 10.0),
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

    async def _load_cached_urls(self):
        """Загружает из pg все url, которые уже получены"""

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "select distinct url from flats where source in ('cian', 'cian_history')"
        )

        for row in cursor.fetchall():
            self.seen_urls.add(row[0])

        cursor.close()
        conn.close()
        print(f"Кэш: {len(self.seen_urls)} URL из БД")

    async def _save_to_db(self, rows: list[dict]):
        """Сохраняет список записей в pg в процессе"""

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        saved = 0

        for row_data in rows:
            row = _build_row(row_data)
            if row.get("price") is None:
                continue
            try:
                cursor.execute(INSERT_SQL, row)
                saved += cursor.rowcount
            except Exception:
                conn.rollback()

        conn.commit()
        cursor.close()
        conn.close()
        return saved

    async def _get_offer_urls(self, page) -> list[str]:
        """Собирает URL объявлений с текущей страницы списка"""
        try:
            await page.wait_for_selector(
                'article[data-name="CardComponent"]', timeout=15000
            )
        except Exception:
            print("Карточки не найдены")
            return []

        SKIP_URL_PARTS = ["/sale/room/", "/sale/share/"]
        SKIP_PHRASES = [
            "продаётся комната",
            "продается комната",
            "продаётся доля",
            "продается доля",
            "комната в ",
            "доля в ",
        ]

        cards = await page.query_selector_all('article[data-name="CardComponent"]')
        urls = []
        for card in cards:
            link = await card.query_selector('div[data-name="LinkArea"] a')
            if not link:
                continue
            href = await link.get_attribute("href")
            if not href or href in self.seen_urls:
                continue

            # Фильтр по URL
            if any(part in href for part in SKIP_URL_PARTS):
                continue

            # Фильтр по тексту карточки
            card_text = (await card.inner_text()).lower()
            if any(phrase in card_text for phrase in SKIP_PHRASES):
                continue

            urls.append(href)
            self.seen_urls.add(href)
        return urls

    async def _parse_offer(self, page, url: str) -> list[dict]:
        """Переходит на страницу объявления, парсит и возвращает dict"""
        await page.goto(url, timeout=30000)
        await page.wait_for_timeout(3000)

        html = await page.content()

        soup_check = BeautifulSoup(html, "html.parser")
        title_el = soup_check.find(attrs={"data-name": "OfferTitleNew"})
        if title_el:
            title_text = title_el.get_text(strip=True).lower()
            if "комната" in title_text or "доля" in title_text:
                return []

        data, price_history = parse_offer_page(html)
        data["url"] = url
        data["source"] = "cian"
        data["parsed_at"] = datetime.now().isoformat()

        rows = [data]

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
        """Формирует URL следующей страницы списка"""
        sep = "&" if "?" in self.base_url else "?"
        return f"{self.base_url}{sep}p={page_num}"

    def save_results(self, filename: str = "cian_offers.json"):
        """Сохраняет результаты в JSON"""
        if not self.results:
            print("Нет данных для сохранения")
            return

        output_path = PROJECT_ROOT / "support_files" / filename
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        print(f"\nСохранено {len(self.results)} записей → {output_path}")

    async def scrape(self, page, label: str = ""):
        """Проходит по страницам списка, собирает и парсит объявления.

        page:  готовая вкладка Playwright (browser.new_page())
        label: метка для логов (напр. "Москва / 1-комн")
        """
        prefix = f"[{label}]" if label else ""

        for page_num in range(1, self.max_pages + 1):
            next_url = self._next_page_url(page_num)
            print(f"\n{prefix} Страница {page_num}/{self.max_pages}: {next_url}")

            try:
                await page.goto(next_url, timeout=30000)
            except Exception as e:
                print(f"{prefix} Ошибка загрузки: {e}")
                break

            urls = await self._get_offer_urls(page)
            if not urls:
                print(f"{prefix} Нет объявлений или CAPTCHA. Стоп.")
                break

            print(f"{prefix} Найдено {len(urls)} объявлений")

            for url in urls:
                try:
                    rows = await self._parse_offer(page, url)
                    if rows:
                        self.results.extend(rows)
                        await self._save_to_db(rows)
                except Exception as e:
                    print(f"{prefix} Ошибка: {e}")

                await asyncio.sleep(random.uniform(*self.delay_range))

            print(f"{prefix} Прогресс: {page_num}/{self.max_pages}")

        print(f"{prefix} Готово. Собрано: {len(self.results)}")


if __name__ == "__main__":
    # Разбиваем по регион × комнатность — обходим лимит 54 стр
    REGIONS = {
        "Москва": "region=1",
        "МО": "region=4593",
    }
    ROOMS = {
        "студии": "room0=1",
        "1-комн": "room1=1",
        "2-комн": "room2=1",
        "3-комн": "room3=1",
        "4-комн": "room4=1",
        "5+комн": "room5=1&room6=1",
    }
    PRICES = {
        # "до5М": "minprice=0&maxprice=5000000",
        # "5-10М": "minprice=5000000&maxprice=10000000",
        "10-20М": "minprice=10000000&maxprice=20000000",
        "20-50М": "minprice=20000000&maxprice=50000000",
        "50М+": "minprice=50000000",
    }

    BASE = "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat"

    # Генерируем: 2 региона × 6 комнат × 5 цен = 60 фильтров × 54 стр ≈ 90K URL
    FILTERS = []
    for reg_name, reg_param in REGIONS.items():
        for room_name, room_param in ROOMS.items():
            for price_name, price_param in PRICES.items():
                FILTERS.append(
                    {
                        "label": f"{reg_name} / {room_name} / {price_name}",
                        "url": f"{BASE}&{reg_param}&{room_param}&{price_param}",
                    }
                )

    print(f"Всего фильтров: {len(FILTERS)}")

    PARALLEL_WORKERS = 4

    async def run_filter(semaphore, context, filt, shared_seen_urls, all_results):
        """Запускает скрапер для одного фильтра. Семафор контролирует кол-во вкладок"""
        async with semaphore:
            page = await context.new_page()
            scraper = CianScraper(base_url=filt["url"], max_pages=54)
            scraper.seen_urls = shared_seen_urls

            print(f"  СТАРТ: {filt['label']}")

            try:
                await scraper.scrape(page, label=filt["label"])
            except Exception as e:
                print(f"[{filt['label']}] Критическая ошибка: {e}")
            finally:
                await page.close()

            all_results.extend(scraper.results)
            print(f"[{filt['label']}] Итого: {len(scraper.results)} записей")

    async def run_all():
        all_results = []
        shared_seen_urls = set()
        semaphore = asyncio.Semaphore(PARALLEL_WORKERS)

        # Загружаем кэш URL из БД
        cache_loader = CianScraper(base_url="", max_pages=0)
        cache_loader.seen_urls = shared_seen_urls
        await cache_loader._load_cached_urls()
        print(f"Кэш загружен: {len(shared_seen_urls)} URL уже в БД")

        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=False)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:137.0) "
                    "Gecko/20100101 Firefox/137.0"
                )
            )

            tasks = [
                run_filter(semaphore, context, filt, shared_seen_urls, all_results)
                for filt in FILTERS
            ]
            await asyncio.gather(*tasks)

            await context.close()
            await browser.close()

        saver = CianScraper(base_url="", max_pages=0)
        saver.results = all_results
        saver.save_results()
        print(f"\nИТОГО: {len(all_results)} записей из {len(shared_seen_urls)} URL")

    asyncio.run(run_all())
