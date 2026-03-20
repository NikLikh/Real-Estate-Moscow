"""
Скрипт-разведчик: открывает первое объявление на cian.ru
и сохраняет HTML страницы в support_files/cian_offer_page.html

Запуск:
    python -m scraper.explore_cian
"""

import asyncio

from playwright.async_api import async_playwright

LISTING_URL = "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=-1"

OUTPUT_PATH = "support_files/cian_offer_page.html"


async def explore():

    async with async_playwright() as p:

        browser = await p.firefox.launch(headless=False)
        page = await browser.new_page()
        await page.goto(LISTING_URL)

        await page.wait_for_selector(
            'article[data-name="CardComponent"]', timeout=15000
        )

        card = await page.query_selector('div[data-name="LinkArea"] a')
        href = await card.get_attribute("href")

        await page.goto(href)
        await page.wait_for_timeout(5000)

        content = await page.content()

        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            f.write(content)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(explore())
