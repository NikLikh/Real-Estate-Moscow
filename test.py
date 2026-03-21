from scraper.parsers import parse_offer_page

with open("support_files/cian_offer_page.html", "r", encoding="utf-8") as f:
    data = parse_offer_page(f.read())

for k, v in data.items():
    if v is not None:
        print(f"{k}: {v}")
