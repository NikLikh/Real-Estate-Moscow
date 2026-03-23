from scraper.parsers_domrf import parse_domrf_offer

with open("support_files/domrf_offer_page.html", encoding="utf-8") as f:
    data = parse_domrf_offer(f.read())

for k, v in data.items():
    if v is not None:
        print(f"{k}: {v}")
