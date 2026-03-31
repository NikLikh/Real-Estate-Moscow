import asyncio
from pathlib import Path

from playwright.async_api import async_playwright

LISTING_URL = "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region=-1"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = PROJECT_ROOT / "support_files" / "cian_offer_page.html"
OFFER_INDX = 0


async def explore():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        await page.goto(LISTING_URL)

        await page.wait_for_selector(
            'article[data-name="CardComponent"]', timeout=15000
        )

        cards = await page.query_selector_all('article[data-name="CardComponent"]')
        link = await cards[OFFER_INDX].query_selector('div[data-name="LinkArea"] a')
        if not link:
            print("Не удалось найти ссылку в карточке")
            await browser.close()
            return

        href = await link.get_attribute("href")
        await page.goto(href)
        await page.wait_for_timeout(5000)

        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            f.write(await page.content())

        await browser.close()


if __name__ == "__main__":
    asyncio.run(explore())
