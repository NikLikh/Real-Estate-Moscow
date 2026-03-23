"""
Парсинг HTML-страниц наш.дом.рф
"""

import re
from unittest import result

from bs4 import BeautifulSoup


def parse_domrf_offer(html: str) -> dict:
    """Парсит HTML страницы квартиры на дом.рф, возвращает dict полей FlatOffer"""
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    data.update(_parse_price_domrf(soup))
    data.update(_parse_flat_domrf(soup))
    data.update(_parse_building_domrf(soup))
    data.update(_parse_meta_domrf(soup))
    return data


def _find(soup, class_contains: str):
    """Поиск элемента по частичному имени styled-component класса"""
    return soup.find(class_=lambda c: c and class_contains in c)


def _find_all(soup, class_contains: str):
    """Поиск всех элементов по частичному имени styled-component класса"""
    return soup.find_all(class_=lambda c: c and class_contains in c)


def _parse_float(text: str) -> float | None:
    match = re.search(r"[\d,\.]+", text)
    return float(match.group().replace(",", ".")) if match else None


def _parse_price_domrf(soup: BeautifulSoup) -> dict:
    """Извлекает цену и цену за м²"""
    result = {"price": None, "price_per_m2": None}

    price_el = _find(soup, "PriceBlock__Price-")
    if price_el:
        digits = re.sub(r"[^\d]", "", price_el.get_text())
        if digits:
            result["price"] = int(digits)

    caption = _find(soup, "PriceBlock__Caption-")
    if caption and "м²" in caption.get_text():
        digits = re.sub(r"[^\d]", "", caption.get_text().split("₽")[0])
        if digits:
            result["price_per_m2"] = int(digits)

    return result


def _parse_flat_domrf(soup: BeautifulSoup) -> dict:
    result = {
        "total_area": None,
        "living_area": None,
        "kitchen_area": None,
        "floor": None,
        "total_floors": None,
        "renovation": None,
        "ceiling_height": None,
        "rooms": None,
    }

    items = _find_all(soup, "Characteristics__PropertyInfo")
    for item in items:
        text = item.get_text(" | ", strip=True)
        if "Общая площадь" in text:
            result["total_area"] = _parse_float(text.split("|")[1])
        elif "Жилая площадь" in text:
            result["living_area"] = _parse_float(text.split("|")[1])
        elif "Площадь кухни" in text:
            result["kitchen_area"] = _parse_float(text.split("|")[1])
        elif "Этаж" in text:
            floor_text = text.split("|")[1]
            if " из " in floor_text:
                floor, total_floors = floor_text.split(" из ")
                result["floor"] = int(floor)
                result["total_floors"] = int(total_floors)
        elif "Отделка" in text:
            result["renovation"] = text.split("|")[1].strip()
        elif "Высота потолков" in text:
            result["ceiling_height"] = _parse_float(text.split("|")[1])

    title = _find(soup, "TitleContainer__Heading")
    if title:
        title_text = title.get_text(strip=True)
        if "студия" in title_text.lower():
            result["rooms"] = -1
        else:
            match = re.search(r"(\d+)-комн", title_text)
            if match:
                result["rooms"] = int(match.group(1))

    return result


def _parse_building_domrf(soup: BeautifulSoup) -> dict:
    result = {
        "residential_complex": None,
        "city": None,
        "district": None,
        "metro_stations": [],
        "completion_date": None,
        "developer": None,
    }

    title = _find(soup, "BuildingCard__Title")
    if title:
        result["residential_complex"] = title.get_text(strip=True)

        if "," in title.get_text(strip=True):
            result["district"] = title.get_text(strip=True).split(",")[1].strip()

    address = _find(soup, "BuildingCard__Address")
    if address:
        city_text = address.get_text(strip=True).split(",")[0].strip()
        result["city"] = city_text.replace("ГОРОД ", "").capitalize()
        if "," in address.get_text(strip=True):
            district = address.get_text(strip=True).split(",")[1].strip()
            result["district"] = district.replace("Район ", "").replace("район ", "")

    metro_names = _find_all(soup, "MetroInfo__Name")
    metro_times = _find_all(soup, "MetroInfo__Time")
    for name, time in zip(metro_names, metro_times):
        time_text = time.get_text(strip=True)
        minutes = (
            int(re.search(r"\d+", time_text).group())
            if re.search(r"\d+", time_text)
            else None
        )
        result["metro_stations"].append(
            {
                "name": name.get_text(strip=True),
                "minutes": minutes,
            }
        )

    completion_date = _find(soup, "BuildingCard__RowValue")

    if completion_date:
        result["completion_date"] = completion_date.get_text(strip=True)

    developer = _find(soup, "Developer__Header-sc")
    if developer:
        result["developer"] = developer.get_text(" ", strip=True)

    return result


def _parse_meta_domrf(soup: BeautifulSoup) -> dict:
    """Извлекает дату обновления"""
    result = {"publication_date": None}

    updated = _find(soup, "PriceBlock__CaptionUpdatedAt")
    match = re.search(r"(\d{2}\.\d{2}\.\d{4})", updated.get_text())
    if match:
        result["publication_date"] = match.group(1)

    return result
