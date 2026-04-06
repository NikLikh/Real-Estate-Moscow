import json
import re

from bs4 import BeautifulSoup


def extract_cian_id(url: str) -> int | None:
    match = re.search(r"/flat/(\d+)", url)
    return int(match.group(1)) if match else None


def parse_offer_page(html: str) -> tuple[dict, list[dict]]:
    soup = BeautifulSoup(html, "lxml")
    data = {}
    data.update(_parse_price(soup))
    data.update(_parse_address(soup))
    data.update(_parse_coordinates(soup))
    data.update(_parse_metro(soup))
    data.update(_parse_summary(soup))
    data.update(_parse_building(soup))
    data.update(_parse_newbuilding(soup))
    data.update(_parse_meta(soup))
    data.update(_parse_extras(soup))
    return data, _parse_price_history(soup)


def _parse_float(text: str) -> float | None:
    match = re.search(r"[\d,\.]+", text)
    return float(match.group().replace(",", ".")) if match else None


def extract_region_id(html: str) -> int | None:
    match = re.search(r'"regionId":\s*(\d+)', html)
    return int(match.group(1)) if match else None


def _parse_coordinates(soup: BeautifulSoup) -> dict:
    result = {"lat": None, "lon": None}
    for s in soup.find_all("script"):
        text = s.string or ""
        if "coordinates" not in text:
            continue
        # циан отдаёт координаты как {"lat":..,"lng":..}
        match = re.search(
            r'"coordinates"\s*:\s*\{\s*"lat"\s*:\s*([\d.]+)\s*,\s*"lng"\s*:\s*([\d.]+)',
            text,
        )
        if match:
            lat, lon = float(match.group(1)), float(match.group(2))
            if lat > 1 and lon > 1:
                result["lat"] = lat
                result["lon"] = lon
                return result
    return result


def _parse_extras(soup: BeautifulSoup) -> dict:
    result = {
        "seller_type": None,
        "photos_count": None,
        "views_count": None,
        "phone_protected": None,
    }

    # seller_type и phone из JSON в script тегах
    full_text = ""
    for s in soup.find_all("script"):
        text = s.string or ""
        if "accountType" in text or "phones" in text:
            full_text = text
            break

    if full_text:
        # accountType: agency/homeowner/developer
        m = re.search(r'"accountType"\s*:\s*"([^"]+)"', full_text)
        if m:
            result["seller_type"] = m.group(1)

        # пустой phones = скрыт, с данными = виден
        m = re.search(r'"phones"\s*:\s*\[([^\]]*)\]', full_text)
        if m:
            result["phone_protected"] = len(m.group(1).strip()) == 0

    # фото из галереи/счетчика
    gallery = soup.find(attrs={"data-name": "GalleryInnerComponent"})
    if gallery:
        imgs = gallery.find_all("img")
        if imgs:
            result["photos_count"] = len(imgs)
    if result["photos_count"] is None:
        counter = soup.find(attrs={"data-name": "PhotoCounter"})
        if counter:
            m = re.search(r"(\d+)", counter.get_text())
            if m:
                result["photos_count"] = int(m.group(1))

    # просмотры
    for el in soup.find_all(attrs={"data-name": "OfferStats"}):
        text = el.get_text(strip=True)
        if "просмотр" in text.lower():
            m = re.search(r"(\d[\d\s]*)", text)
            if m:
                result["views_count"] = int(m.group(1).replace(" ", ""))
            break

    return result


def _parse_price(soup: BeautifulSoup) -> dict:
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

    for fact in soup.find_all(attrs={"data-name": "OfferFactItem"}):
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
    result = {
        "region": None,
        "municipality": None,
        "district": None,
        "microdistrict": None,
        "street": None,
        "house": None,
    }

    container = soup.find(attrs={"data-name": "AddressContainer"})
    if not container:
        return result

    items = container.find_all(attrs={"data-name": "AddressItem"})
    texts = []
    for item in items:
        link = item.find("a")
        texts.append(link.get_text(strip=True) if link else item.get_text(strip=True))

    if not texts:
        return result

    # texts[0] всегда регион
    result["region"] = texts[0]

    # последний элемент = дом, если короткий и содержит цифры
    tail = []
    if len(texts) >= 3:
        last = texts[-1]
        if len(last) < 15 and re.search(r"\d", last):
            result["house"] = last
            tail = texts[1:-1]
        else:
            tail = texts[1:]
    elif len(texts) == 2:
        tail = texts[1:]
    else:
        return result

    # последний в tail = улица
    street_keywords = ["ул.", "улица", "просп", "бульв", "шоссе", "переул", "наб.", "проезд", "тупик"]
    if tail:
        last_tail = tail[-1]
        if any(kw in last_tail.lower() for kw in street_keywords) or (result["house"] and len(tail) >= 2):
            result["street"] = last_tail
            tail = tail[:-1]

    # остаток распределяем по муниципалитету, району, микрорайону
    micro_keywords = ["мкр", "микрорайон", "квартал", "жилой комплекс"]
    for i, t in enumerate(tail):
        if i == 0:
            result["municipality"] = t
        elif any(kw in t.lower() for kw in micro_keywords):
            result["microdistrict"] = t
        elif not result["district"]:
            result["district"] = t
        elif not result["microdistrict"]:
            result["microdistrict"] = t

    return result


def _parse_metro(soup: BeautifulSoup) -> dict:
    stations = []

    for item in soup.find_all(attrs={"data-name": "UndergroundItem"}):
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
        "is_apartments": None,
    }

    # циан пишет то "Ремонт" то "Отделка", маппим в наши поля
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

    items = soup.find_all(attrs={"data-name": "OfferSummaryInfoItem"})

    for item in items:
        ps = item.find_all("p", recursive=False)
        if len(ps) < 2:
            continue
        label = ps[0].get_text(strip=True)
        value = ps[1].get_text(strip=True)

        if label in mapping:
            field = mapping[label]
            if field in ("total_area", "living_area", "kitchen_area", "ceiling_height"):
                result[field] = _parse_float(value)
            else:
                result[field] = value

        if label == "Тип жилья":
            result["is_new_building"] = value == "Новостройка"

        if label == "Год постройки":
            parsed = _parse_float(value)
            if parsed is not None:
                result["year_built"] = int(parsed)

    title = soup.find(attrs={"data-name": "OfferTitleNew"})
    if title:
        title_text = title.get_text(strip=True)
        if "студия" in title_text.lower():
            result["rooms"] = -1  # студия = -1, чтобы отличать от "не указано" (None)
        else:
            match = re.search(r"(\d+)-комн", title_text)
            if match:
                result["rooms"] = int(match.group(1))

        result["is_apartments"] = "апартаменты" in title_text.lower()

    for item in soup.find_all(attrs={"data-name": "ObjectFactoidsItem"}):
        text = item.get_text(strip=True)
        if "этаж" in text.lower():
            match = re.search(r"(\d+)\s*из\s*(\d+)", text)
            if match:
                result["floor"] = int(match.group(1))
                result["total_floors"] = int(match.group(2))
                break

    return result


def _parse_building(soup: BeautifulSoup) -> dict:
    result = {"building_type": None, "parking": None, "elevators": None}

    mapping = {
        "Тип дома": "building_type",
        "Парковка": "parking",
        "Количество лифтов": "elevators",
    }

    for item in soup.find_all(attrs={"data-name": "OfferSummaryInfoItem"}):
        ps = item.find_all("p", recursive=False)
        if len(ps) < 2:
            continue
        label = ps[0].get_text(strip=True)
        value = ps[1].get_text(strip=True)
        if label in mapping:
            result[mapping[label]] = value

    return result


def _parse_newbuilding(soup: BeautifulSoup) -> dict:
    result = {
        "developer": None,
        "residential_complex": None,
        "completion_date": None,
    }

    title = soup.find(attrs={"data-name": "OfferTitleNew"})
    if title:
        match = re.search(r"ЖК\s*[«](.*?)[»]", title.get_text(strip=True))
        if match:
            result["residential_complex"] = match.group(1)

    specs = soup.find(attrs={"data-name": "NewbuildingSpecifications"})
    if specs:
        elements = specs.find_all(["span", "a"])
        for i, el in enumerate(elements[:-1]):
            label = el.get_text(strip=True)
            value = elements[i + 1].get_text(strip=True)
            if label == "Застройщик":
                result["developer"] = value
            elif label == "Сдача комплекса":
                result["completion_date"] = value

    return result


def _parse_meta(soup: BeautifulSoup) -> dict:
    result = {"publication_date": None, "description": None}

    pub_date_el = soup.find(attrs={"data-testid": "metadata-updated-date"})
    if pub_date_el:
        result["publication_date"] = pub_date_el.get_text(strip=True)

    description_el = soup.find(attrs={"data-name": "Description"})
    if description_el:
        result["description"] = description_el.get_text(strip=True)

    return result


def parse_offer_from_json(html: str) -> tuple[dict | None, list[dict]]:
    blob = _find_offer_json(html)
    if not blob:
        return None, []

    data = _map_json_to_fields(blob)
    if not data.get("price"):
        return None, []

    # seller_type и phones из отдельных JSON-блоков
    m = re.search(r'"accountType"\s*:\s*"([^"]+)"', html)
    data["seller_type"] = m.group(1) if m else None

    m = re.search(r'"phones"\s*:\s*\[([^\]]*)\]', html)
    data["phone_protected"] = len(m.group(1).strip()) == 0 if m else None

    price_history = _extract_price_history_raw(html)
    return data, price_history


def _find_offer_json(html: str) -> dict | None:
    best = None
    best_len = 0
    for m in re.finditer(r'"cianId":\d+', html):
        pos = m.start()
        # ищем начало объемлющего JSON
        depth = 0
        start = pos
        for i in range(pos, max(pos - 100000, -1), -1):
            if html[i] == '}':
                depth += 1
            elif html[i] == '{':
                depth -= 1
                if depth < 0:
                    start = i
                    break
        # ищем конец
        depth = 0
        end = pos
        for i in range(start, min(start + 200000, len(html))):
            if html[i] == '{':
                depth += 1
            elif html[i] == '}':
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        if end - start > best_len:
            best_len = end - start
            try:
                best = json.loads(html[start:end])
            except (json.JSONDecodeError, RecursionError):
                pass
    return best


def _map_json_to_fields(j: dict) -> dict:
    geo = j.get("geo") or {}
    coords = geo.get("coordinates") or {}
    building = j.get("building") or {}
    terms = j.get("bargainTerms") or {}
    nb = j.get("newbuilding") or {}
    jk = geo.get("jk") or {}

    # адрес из массива location
    addr = _parse_address_from_json(geo.get("address") or [])

    # метро
    metros = []
    for u in geo.get("undergrounds") or []:
        name = u.get("name")
        if name:
            metros.append((name, u.get("time")))

    # циан: 0 = студия, в БД храним как -1
    rooms_raw = j.get("roomsCount")
    rooms = -1 if rooms_raw == 0 else rooms_raw

    # площади
    total_area = j.get("totalArea")
    if isinstance(total_area, str):
        total_area = _parse_float(total_area)

    # цена за м2
    price = terms.get("price")
    price_per_m2 = None
    if price and total_area and total_area > 0:
        price_per_m2 = int(price / total_area)

    # санузлы
    wc_combined = j.get("combinedWcsCount") or 0
    wc_separate = j.get("separateWcsCount") or 0
    bathrooms = None
    if wc_combined or wc_separate:
        parts = []
        if wc_separate:
            parts.append(f"{wc_separate} разд.")
        if wc_combined:
            parts.append(f"{wc_combined} совм.")
        bathrooms = ", ".join(parts)

    # балкон/лоджия
    balc = j.get("balconiesCount") or 0
    logg = j.get("loggiasCount") or 0
    balcony = None
    if balc or logg:
        parts = []
        if balc:
            parts.append(f"{balc} балк.")
        if logg:
            parts.append(f"{logg} лодж.")
        balcony = ", ".join(parts)

    return {
        "price": price,
        "price_per_m2": price_per_m2,
        "deal_conditions": terms.get("saleType"),
        **addr,
        "lat": coords.get("lat"),
        "lon": coords.get("lng"),
        "metro_stations": metros if metros else None,
        "rooms": rooms,
        "total_area": total_area,
        "living_area": j.get("livingArea"),
        "kitchen_area": j.get("kitchenArea"),
        "floor": j.get("floorNumber"),
        "total_floors": building.get("floorsCount"),
        "ceiling_height": building.get("ceilingHeight"),
        "renovation": j.get("repairType"),
        "bathrooms": bathrooms,
        "balcony": balcony,
        "window_view": j.get("windowsViewType"),
        "is_apartments": j.get("isApartments"),
        "year_built": building.get("buildYear"),
        "building_type": building.get("materialType"),
        "parking": building["parking"].get("type") if isinstance(building.get("parking"), dict) and building["parking"] else None,
        "is_new_building": nb.get("isFromDeveloper") or nb.get("isFromBuilder"),
        "developer": nb.get("name") if nb.get("name") else None,
        "residential_complex": jk.get("name") if jk.get("name") else None,
        "completion_date": None,
        "description": j.get("description"),
        "publication_date": j.get("creationDate"),
        "phone_protected": None,
    }


def _parse_address_from_json(items: list) -> dict:
    result = {
        "region": None, "municipality": None, "district": None,
        "microdistrict": None, "street": None, "house": None,
    }
    texts = [it.get("shortName") or it.get("fullName") or "" for it in items]
    if not texts:
        return result

    result["region"] = texts[0]

    tail = []
    if len(texts) >= 3:
        last = texts[-1]
        if len(last) < 15 and re.search(r"\d", last):
            result["house"] = last
            tail = texts[1:-1]
        else:
            tail = texts[1:]
    elif len(texts) == 2:
        tail = texts[1:]
    else:
        return result

    street_kw = ["ул.", "улица", "просп", "бульв", "шоссе", "переул", "наб.", "проезд", "тупик"]
    if tail:
        last_tail = tail[-1]
        if any(kw in last_tail.lower() for kw in street_kw) or (result["house"] and len(tail) >= 2):
            result["street"] = last_tail
            tail = tail[:-1]

    micro_kw = ["мкр", "микрорайон", "квартал", "жилой комплекс"]
    for i, t in enumerate(tail):
        if i == 0:
            result["municipality"] = t
        elif any(kw in t.lower() for kw in micro_kw):
            result["microdistrict"] = t
        elif not result["district"]:
            result["district"] = t
        elif not result["microdistrict"]:
            result["microdistrict"] = t

    return result


def _extract_price_history_raw(html: str) -> list[dict]:
    m = re.search(r'"priceChanges":\s*(\[.*?\])', html)
    if not m:
        return []
    try:
        return [
            {"price": item["priceData"]["price"], "date": item["changeTime"][:10]}
            for item in json.loads(m.group(1))
        ]
    except (json.JSONDecodeError, KeyError):
        return []


def parse_similar_urls_from_html(html: str) -> list[str]:
    raw = re.findall(r'href="(https?://[^"]*?/sale/flat/\d+/)"', html)
    seen = set()
    result = []
    for u in raw:
        clean = u.split("?")[0]
        if clean not in seen:
            seen.add(clean)
            result.append(clean)
    return result


def _parse_price_history(soup: BeautifulSoup) -> list[dict]:
    for s in soup.find_all("script"):
        text = s.string or ""
        if "priceChanges" not in text:
            continue
        match = re.search(r'"priceChanges":\s*(\[.*?\])', text)
        if not match:
            continue
        try:
            return [
                {"price": item["priceData"]["price"], "date": item["changeTime"][:10]}
                for item in json.loads(match.group(1))
            ]
        except (json.JSONDecodeError, KeyError):
            pass
    return []
