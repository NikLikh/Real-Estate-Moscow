"""
Скрапер объявлений cian.ru

Запуск:
    python -m scraper.cian
"""

import asyncio
import random
from datetime import datetime

from bs4 import BeautifulSoup
from camoufox.async_api import AsyncCamoufox

from db.loader import get_cached_urls, save_rows
from scraper.parsers import parse_offer_page
from scraper.utils import (
    clear_checkpoint,
    install_shutdown_handler,
    is_shutting_down,
    load_checkpoint,
    managed_page,
    save_checkpoint,
)


class CianScraper:
    def __init__(
        self,
        base_url: str,
        max_pages: int = 3,
        delay_range: tuple[float, float] = (5.0, 10.0),
        headless: bool = False,
    ):
        self.base_url = base_url
        self.max_pages = max_pages
        self.delay_range = delay_range
        self.headless = headless

        self.results: list[dict] = []
        self.seen_urls: set[str] = set()

    def _load_cached_urls(self):
        self.seen_urls.update(get_cached_urls(["cian", "cian_history"]))
        print(f"Кэш: {len(self.seen_urls)} URL из БД")

    def _save_to_db(self, rows: list[dict]) -> int:
        return save_rows(rows)

    SKIP_URL_PARTS = ["/sale/room/", "/sale/share/"]
    SKIP_PHRASES = [
        "продаётся комната",
        "продается комната",
        "продаётся доля",
        "продается доля",
        "комната в ",
        "доля в ",
    ]

    async def _wait_for_captcha(self, page):
        """Если на странице капча - ждем, пока человек решит."""
        content = await page.content()
        title = await page.title()

        captcha_signs = ["captcha", "datadome", "не робот", "robot"]
        text = (content + title).lower()

        if not any(sign in text for sign in captcha_signs):
            return

        print("  КАПЧА! Решите в браузере и нажмите Enter...")
        await asyncio.to_thread(input)
        await page.wait_for_timeout(2000)

    async def _get_offer_urls(self, page) -> list[str]:
        """Собирает URL объявлений со страницы списка."""
        try:
            await page.wait_for_selector(
                'article[data-name="CardComponent"]', timeout=15000
            )
        except Exception:
            # может быть капча, даем шанс решить вручную
            await self._wait_for_captcha(page)
            try:
                await page.wait_for_selector(
                    'article[data-name="CardComponent"]', timeout=15000
                )
            except Exception:
                print("  Карточки не найдены")
                return []

        cards = await page.query_selector_all('article[data-name="CardComponent"]')
        urls = []

        for card in cards:
            link = await card.query_selector('div[data-name="LinkArea"] a')
            if not link:
                continue

            href = await link.get_attribute("href")
            if not href or href in self.seen_urls:
                continue

            if any(part in href for part in self.SKIP_URL_PARTS):
                continue

            card_text = (await card.inner_text()).lower()
            if any(phrase in card_text for phrase in self.SKIP_PHRASES):
                continue

            urls.append(href)
            self.seen_urls.add(href)

        return urls

    async def _parse_offer(self, page, url: str) -> list[dict]:
        """Парсит страницу объявления и историю цен."""
        await page.goto(url, timeout=30000)
        await page.wait_for_timeout(3000)

        html = await page.content()

        # доп проверка - вдруг это комната/доля
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

        # исторические цены идут отдельными записями
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

    async def scrape(self, page, label: str = ""):
        """Проходит по страницам листинга и парсит объявления."""
        prefix = f"[{label}]" if label else ""

        for page_num in range(1, self.max_pages + 1):
            if is_shutting_down():
                print(f"{prefix} Остановка по сигналу на стр. {page_num}")
                break

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
                if is_shutting_down():
                    break

                try:
                    rows = await self._parse_offer(page, url)
                    if rows:
                        self.results.extend(rows)
                        saved = self._save_to_db(rows)
                        if saved:
                            print(f"{prefix} +{saved} в БД")
                except Exception as e:
                    print(f"{prefix} Ошибка: {e}")

                await asyncio.sleep(random.uniform(*self.delay_range))

        print(f"{prefix} Готово. Собрано: {len(self.results)}")

    def _next_page_url(self, page_num: int) -> str:
        sep = "&" if "?" in self.base_url else "?"
        return f"{self.base_url}{sep}p={page_num}"


CIAN_REGIONS = {
    "Москва": "region=1",
    "МО": "region=4593",
}
CIAN_ROOMS = {
    "студии": "room0=1",
    "1-комн": "room1=1",
    "2-комн": "room2=1",
    "3-комн": "room3=1",
    "4-комн": "room4=1",
    "5+комн": "room5=1&room6=1",
}
CIAN_PRICES = {
    "10-20М": "minprice=10000000&maxprice=20000000",
    "20-50М": "minprice=20000000&maxprice=50000000",
    "50М+": "minprice=50000000",
}
CIAN_BASE = "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat"
PARALLEL_WORKERS = 2


def _build_filters():
    filters = []
    for reg_name, reg_param in CIAN_REGIONS.items():
        for room_name, room_param in CIAN_ROOMS.items():
            for price_name, price_param in CIAN_PRICES.items():
                filters.append(
                    {
                        "label": f"{reg_name} / {room_name} / {price_name}",
                        "url": f"{CIAN_BASE}&{reg_param}&{room_param}&{price_param}",
                    }
                )
    return filters


async def _worker(name, queue, context, seen_urls, all_results, completed_filters):
    async with managed_page(context) as page:
        while not queue.empty() and not is_shutting_down():
            filt = queue.get_nowait()
            label = filt["label"]
            print(f"\n[{name}] СТАРТ: {label}")

            scraper = CianScraper(base_url=filt["url"], max_pages=54)
            scraper.seen_urls = seen_urls

            try:
                await scraper.scrape(page, label=label)
            except Exception as e:
                print(f"[{name}] Критическая ошибка ({label}): {e}")

            all_results.extend(scraper.results)

            if not is_shutting_down():
                completed_filters.append(label)
                save_checkpoint("cian", {"completed_filters": completed_filters})

            print(f"[{name}] {label}: {len(scraper.results)} записей")


async def main():
    install_shutdown_handler()

    all_results = []
    seen_urls = get_cached_urls(["cian", "cian_history"])
    print(f"Кэш загружен: {len(seen_urls)} URL")

    # пропускаем уже обработанные фильтры
    all_filters = _build_filters()
    checkpoint = load_checkpoint("cian")
    completed_labels = set(checkpoint.get("completed_filters", [])) if checkpoint else set()
    if completed_labels:
        print(f"Чекпоинт: {len(completed_labels)} фильтров уже обработано")

    remaining = [f for f in all_filters if f["label"] not in completed_labels]
    completed_filters = list(completed_labels)
    print(f"Осталось фильтров: {len(remaining)} из {len(all_filters)}")

    if not remaining:
        print("Все фильтры обработаны!")
        clear_checkpoint("cian")
        return

    queue = asyncio.Queue()
    for filt in remaining:
        queue.put_nowait(filt)

    async with AsyncCamoufox(headless=False, locale="ru-RU", humanize=True) as browser:
        context = await browser.new_context()

        blocked = {"image", "media", "font"}
        await context.route(
            "**/*",
            lambda route: (
                route.abort()
                if route.request.resource_type in blocked
                else route.continue_()
            ),
        )

        try:
            workers = [
                asyncio.create_task(
                    _worker(
                        f"W{i+1}", queue, context,
                        seen_urls, all_results, completed_filters,
                    )
                )
                for i in range(PARALLEL_WORKERS)
            ]
            await asyncio.gather(*workers)
        finally:
            await context.close()

    if not is_shutting_down():
        clear_checkpoint("cian")

    print(f"\n{'='*50}")
    print(f"ИТОГО: {len(all_results)} записей из {len(seen_urls)} URL")
    if is_shutting_down():
        print("Остановлено по сигналу. При перезапуске продолжит с чекпоинта.")
    print(f"{'='*50}")


if __name__ == "__main__":
    asyncio.run(main())
