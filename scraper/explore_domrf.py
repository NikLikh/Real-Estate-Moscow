import asyncio
from pathlib import Path

from playwright.async_api import async_playwright

LISTING_URL = (
    "https://xn--80az8a.xn--d1aqf.xn--p1ai/"
    "%D1%81%D0%B5%D1%80%D0%B2%D0%B8%D1%81%D1%8B/"
    "%D0%BA%D0%B0%D1%82%D0%B0%D0%BB%D0%BE%D0%B3-%D0%BA%D0%B2%D0%B0%D1%80%D1%82%D0%B8%D1%80/"
    "%D1%81%D0%BF%D0%B8%D1%81%D0%BE%D0%BA"
    "?flatStatus=free%2Cbooked&page=0&limit=20&place=0-1"
)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_LISTING = PROJECT_ROOT / "support_files" / "domrf_listing_page.html"
OUTPUT_OFFER = PROJECT_ROOT / "support_files" / "domrf_offer_page.html"


async def explore():
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=False)
        page = await browser.new_page()
        await page.goto(LISTING_URL)
        await page.wait_for_timeout(15000)

        with open(OUTPUT_LISTING, "w", encoding="utf-8") as f:
            f.write(await page.content())

        link = await page.query_selector('a[href*="/квартира/"]')
        if not link:
            print("Не удалось найти ссылку на квартиру")
            await browser.close()
            return

        href = await link.get_attribute("href")
        if href and not href.startswith("http"):
            href = "https://xn--80az8a.xn--d1aqf.xn--p1ai" + href

        await page.goto(href)
        await page.wait_for_timeout(15000)

        with open(OUTPUT_OFFER, "w", encoding="utf-8") as f:
            f.write(await page.content())

        await browser.close()


if __name__ == "__main__":
    asyncio.run(explore())
