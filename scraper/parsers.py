"""
Парсинг HTML-страниц cian
"""

import re
from unittest import result

from bs4 import BeautifulSoup


def parse_offer_page(html: str) -> dict:
    """Парсит HTML страницы объявления cian.ru, возвращает dict полей FlatOffer."""
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    data.update(_parse_price(soup))
    data.update(_parse_address(soup))
    data.update(_parse_metro(soup))
    data.update(_parse_summary(soup))
    data.update(_parse_building(soup))
    data.update(_parse_newbuilding(soup))
    data.update(_parse_meta(soup))
    return data


def _parse_float(text: str) -> float | None:
    """Парсит число из строки, например '44,1 м2' → 44.1"""
    match = re.search(r"[\d,\.]+", text)
    return float(match.group().replace(",", ".")) if match else None


def _parse_price(soup: BeautifulSoup) -> dict:
    """Извлекает цену, цену за м2, скидку, условия сделки."""
    result = {
        "price": None,
        "price_per_m2": None,
        "discount_pct": None,
        "deal_conditions": None,
    }

    price_el = soup.find(attrs={"data-testid": "price-amount"})
    if price_el:
        digits = re.sub(r"[^\d]", "", price_el.get_text())
        if digits:
            result["price"] = int(digits)

    facts = soup.find_all(attrs={"data-name": "OfferFactItem"})
    for fact in facts:
        text = fact.get_text(" ", strip=True)
        if "₽/м²" in text:
            digits = re.sub(r"[^\d]", "", text.split("₽")[0])
            if digits:
                result["price_per_m2"] = int(digits)
        elif any(w in text for w in ["сделки", "участие", "договор"]):
            result["deal_conditions"] = (
                text.split("сделки")[-1].strip() if "сделки" in text else text
            )

    discount_el = soup.find(attrs={"data-name": "PriceDiscount"})
    if discount_el:
        match = re.search(r"-(\d+)%", discount_el.get_text())
        if match:
            result["discount_pct"] = int(match.group(1))

    return result


def _parse_address(soup: BeautifulSoup) -> dict:
    """Извлекает адрес: город, округ, район, улица, дом."""
    result = {
        "city": None,
        "region": None,
        "district": None,
        "street": None,
        "house_number": None,
    }

    container = soup.find(attrs={"data-name": "AddressContainer"})
    if not container:
        return result

    items = container.find_all(attrs={"data-name": "AddressItem"})
    texts = []
    for item in items:
        link = item.find("a")
        texts.append(link.get_text(strip=True) if link else item.get_text(strip=True))

    if len(texts) >= 1:
        result["city"] = texts[0]
    if len(texts) >= 2:
        result["region"] = texts[1]
    if len(texts) >= 3:
        result["district"] = texts[2]
    if len(texts) >= 4:
        result["street"] = texts[3]
    if len(texts) >= 5:
        result["house_number"] = texts[4]

    return result


def _parse_metro(soup: BeautifulSoup) -> dict:
    """Извлекает список станций метро с временем до них в минутах."""
    stations = []
    metro_items = soup.find_all(attrs={"data-name": "UndergroundItem"})

    for item in metro_items:
        name_el = item.find("a")
        name = name_el.get_text(strip=True) if name_el else None
        if not name:
            continue

        minutes = None
        for span in item.find_all("span"):
            if "мин" in span.get_text():
                match = re.search(r"(\d+)", span.get_text())
                if match:
                    minutes = int(match.group(1))
                break

        stations.append((name, minutes))

    transport_score = None
    ta = soup.find(attrs={"data-name": "TransportAccessibilityEntry"})
    if ta:
        match = re.search(r"([\d,\.]+)\s*из\s*10", ta.get_text())
        if match:
            transport_score = float(match.group(1).replace(",", "."))

    return {
        "metro_stations": stations if stations else None,
        "transport_score": transport_score,
    }


def _parse_summary(soup: BeautifulSoup) -> dict:
    """
    Извлекает характеристики квартиры: площадь, ремонт, санузел, балкон, вид из окон.
    """
    result = {
        "rooms": None,
        "total_area": None,
        "living_area": None,
        "kitchen_area": None,
        "ceiling_height": None,
        "renovation": None,
        "bathrooms": None,
        "balcony": None,
        "window_view": None,
        "is_apartment": None,
        "description": None,
    }

    items = soup.find_all(attrs={"data-name": "OfferSummaryInfoItem"})

    for item in items:
        ps = item.find_all("p", recursive=False)
        if len(ps) < 2:
            continue
        label = ps[0].get_text(strip=True)
        value = ps[1].get_text(strip=True)

        mapping = {
            "Общая площадь": "total_area",
            "Жилая площадь": "living_area",
            "Площадь кухни": "kitchen_area",
            "Высота потолков": "ceiling_height",
            "Ремонт": "renovation",
            "Отделка": "renovation",
            "Санузел": "bathrooms",
            "Балкон/лоджия": "balcony",
            "Вид из окон": "window_view",
        }

        if label in mapping:
            field = mapping[label]
            if field in ["total_area", "living_area", "kitchen_area"]:
                result[field] = _parse_float(value)
            elif field == "ceiling_height":
                result[field] = _parse_float(value)
            else:
                result[field] = value

    return result


def _parse_building(soup: BeautifulSoup) -> dict:
    """Извлекает характеристики дома из OfferSummaryInfoItem."""
    return {}


def _parse_newbuilding(soup: BeautifulSoup) -> dict:
    """Извлекает данные о ЖК и застройщике (только для новостроек)."""
    return {}


def _parse_meta(soup: BeautifulSoup) -> dict:
    """Извлекает мета-данные: дату обновления, описание."""
    return {}
