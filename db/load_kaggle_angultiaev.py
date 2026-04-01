"""
Загрузка JSON из Kaggle-датасета angultiaev/flat-sale-m24ml.

Датасет ~162 ГБ (квартиры Москвы), train/{hash}/data.json + img/*.jpeg.
Скачиваем только data.json через remotezip (Range requests), fallback - stream-unzip.

python -m db.load_kaggle_angultiaev
"""

import json
import re
from pathlib import Path

import psycopg2

from db.loader import DB_CONFIG, INSERT_SQL, _build_row
from scraper.utils import save_checkpoint, load_checkpoint, clear_checkpoint

DATASET_HANDLE = "angultiaev/flat-sale-m24ml"
DOWNLOAD_URL = (
    "https://www.kaggle.com/api/v1/datasets/download/angultiaev/flat-sale-m24ml"
)
SOURCE_NAME = "kaggle_angultiaev"
BATCH_SIZE = 500
CHECKPOINT_EVERY = 1000


def _get_kaggle_auth() -> tuple[str, str]:
    """Возвращает (username, key) для Kaggle API.
    Ищет в env, ~/.kaggle/kaggle.json, или спрашивает интерактивно."""
    import os

    # env vars
    username = os.environ.get("KAGGLE_USERNAME")
    key = os.environ.get("KAGGLE_KEY")
    if username and key:
        return username, key

    # kaggle.json
    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    if kaggle_json.exists():
        with open(kaggle_json, encoding="utf-8") as f:
            creds = json.load(f)
        return creds["username"], creds["key"]

    # интерактивный ввод
    print("Kaggle API credentials не найдены.")
    print("Получите их: kaggle.com -> Settings -> API -> Create New Token")
    print("Или введите вручную:")
    username = input("  Kaggle username: ").strip()
    key = input("  Kaggle API key: ").strip()
    if not username or not key:
        raise ValueError("Credentials не могут быть пустыми")

    # сохраним на будущее
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(parents=True, exist_ok=True)
    kaggle_json = kaggle_dir / "kaggle.json"
    with open(kaggle_json, "w", encoding="utf-8") as f:
        json.dump({"username": username, "key": key}, f)
    print(f"  Сохранено в {kaggle_json}")

    return username, key


def _parse_price(raw: str | None) -> int | None:
    """'13 000 000 руб' -> 13000000"""
    if not raw:
        return None
    digits = re.sub(r"[^\d]", "", raw)
    return int(digits) if digits else None


def _parse_area(raw: str | None) -> float | None:
    """'28 м2' -> 28.0, '28,5 м2' -> 28.5"""
    if not raw:
        return None
    m = re.search(r"([\d]+[.,]?\d*)", raw)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def _parse_floor(raw: str | None) -> tuple[int | None, int | None]:
    """'4 из 8' -> (4, 8)"""
    if not raw:
        return None, None
    m = re.search(r"(\d+)\s*из\s*(\d+)", raw)
    if m:
        return int(m.group(1)), int(m.group(2))
    # только число - считаем этажом
    m2 = re.search(r"(\d+)", raw)
    return (int(m2.group(1)), None) if m2 else (None, None)


def _parse_ceiling(raw: str | None) -> float | None:
    """'2,64 м' -> 2.64"""
    return _parse_area(raw)


def _parse_rooms(title: str | None) -> int | None:
    """'Продается 1-комн. квартира, 28 м2' -> 1, 'студия' -> 0"""
    if not title:
        return None
    t = title.lower()
    if "студи" in t:
        return 0
    m = re.search(r"(\d+)\s*-?\s*комн", t)
    return int(m.group(1)) if m else None


def _parse_address(raw: str | None) -> dict:
    """Разбирает адрес вида 'Москва, ВАО, р-н ..., ул., 21' по позициям."""
    result = {"city": None, "region": None, "district": None,
              "street": None, "house_number": None}
    if not raw:
        return result

    parts = [p.strip() for p in raw.split(",")]
    if not parts:
        return result

    result["city"] = parts[0]

    # ищем район, округ, улицу, дом по паттернам
    remaining = parts[1:]
    for part in remaining:
        if re.search(r"р-н|район", part, re.IGNORECASE):
            result["district"] = part
        elif re.match(r"^[А-ЯЁA-Z]{2,5}$", part.strip()):
            result["region"] = part
        elif re.search(r"ул\.|улица|пер\.|переулок|просп|пр-т|ш\.|шоссе|б-р|бульвар|наб\.|набережная|пр-д|проезд", part, re.IGNORECASE):
            result["street"] = part
        elif re.match(r"^\d", part.strip()) and result["street"] and not result["house_number"]:
            result["house_number"] = part

    # если улица не нашлась по паттерну - берем оставшиеся части
    if not result["street"] and len(remaining) >= 2:
        # предпоследний элемент - часто улица
        for part in remaining:
            if part != result.get("region") and part != result.get("district"):
                if not result["street"]:
                    result["street"] = part
                elif not result["house_number"]:
                    result["house_number"] = part

    return result


def _parse_metro(stations: list | None) -> list | None:
    """Преобразует список станций в формат [name, minutes] для JSONB."""
    if not stations:
        return None
    result = []
    for s in stations:
        name = s.get("station_name", "")
        time_raw = s.get("station_time", "")
        minutes = None
        m = re.search(r"(\d+)", str(time_raw))
        if m:
            minutes = int(m.group(1))
        result.append([name, minutes])
    return result if result else None


def _parse_year(raw: str | None) -> int | None:
    if not raw:
        return None
    m = re.search(r"(\d{4})", str(raw))
    return int(m.group(1)) if m else None


def _transform(data: dict, hash_id: str) -> dict:
    """data.json -> dict для _build_row()."""
    summary = data.get("offer_summary") or {}
    details = data.get("offer_details") or {}

    floor, total_floors = _parse_floor(summary.get("Этаж"))
    addr = _parse_address(data.get("address"))

    housing_type = summary.get("Тип жилья", "")
    is_new = True if "новостройка" in housing_type.lower() else (
        False if housing_type else None
    )

    metro = _parse_metro(data.get("nearest_stations"))

    record = {
        "url": f"kaggle_angultiaev_{hash_id}",
        "source": SOURCE_NAME,
        "price": _parse_price(data.get("price")),
        "price_per_m2": _parse_price(details.get("Цена за метр")),
        "discount_pct": None,
        "deal_conditions": details.get("Условия сделки"),
        "city": addr["city"],
        "region": addr["region"],
        "district": addr["district"],
        "street": addr["street"],
        "house_number": addr["house_number"],
        "lat": None,
        "lon": None,
        "metro_stations": metro,
        "transport_score": None,
        "rooms": _parse_rooms(data.get("title")),
        "total_area": _parse_area(summary.get("Общая площадь")),
        "living_area": _parse_area(summary.get("Жилая площадь")),
        "kitchen_area": _parse_area(summary.get("Площадь кухни")),
        "floor": floor,
        "total_floors": total_floors,
        "ceiling_height": _parse_ceiling(summary.get("Высота потолков")),
        "renovation": summary.get("Ремонт"),
        "bathrooms": summary.get("Санузел"),
        "balcony": summary.get("Балкон/лоджия"),
        "window_view": summary.get("Вид из окон"),
        "is_apartments": None,
        "year_built": _parse_year(summary.get("Год постройки")),
        "building_type": summary.get("Тип дома"),
        "parking": summary.get("Парковка"),
        "elevators": summary.get("Количество лифтов"),
        "is_new_building": is_new,
        "developer": None,
        "residential_complex": None,
        "completion_date": summary.get("Год сдачи"),
        "description": data.get("description"),
        "publication_date": None,
    }
    return record


def _extract_hash_id(zip_path: str) -> str | None:
    """'train/train/0000c1deb1198f7b.../data.json' -> '0000c1deb1198f7b...'"""
    parts = zip_path.replace("\\", "/").split("/")
    # data.json - последний, hash_id - предпоследний
    if len(parts) >= 2 and parts[-1] == "data.json":
        return parts[-2]
    return None


def _flush_batch(cursor, batch: list[dict]) -> tuple[int, int, int]:
    """Вставляет батч, возвращает (inserted, duplicates, no_price)."""
    if not batch:
        return 0, 0, 0

    rows = []
    no_price = 0
    for record in batch:
        row = _build_row(record)
        if row.get("price") is None:
            no_price += 1
            continue
        rows.append(row)

    if not rows:
        return 0, 0, no_price

    inserted = 0
    for row in rows:
        try:
            cursor.execute(INSERT_SQL, row)
            inserted += cursor.rowcount
        except Exception as e:
            cursor.connection.rollback()
            print(f"  Ошибка вставки {row.get('url', '?')}: {e}")

    return inserted, len(rows) - inserted, no_price


def _resolve_gcs_url(kaggle_url: str, auth: tuple) -> str:
    """Kaggle API редиректит на подписанный GCS URL (временный, ~пару часов)."""
    import requests

    resp = requests.get(kaggle_url, auth=auth, stream=True, timeout=60,
                        allow_redirects=True)
    resp.raise_for_status()
    gcs_url = resp.url
    resp.close()
    print(f"GCS URL получен (подпись временная)")
    return gcs_url


def _load_via_remotezip(url: str, auth: tuple, loaded_ids: set) -> dict:
    """Скачивает только data.json через HTTP Range requests."""
    from remotezip import RemoteZip

    stats = {"processed": 0, "inserted": 0, "skipped": 0, "no_price": 0, "errors": 0}

    gcs_url = _resolve_gcs_url(url, auth)

    print("Подключаемся к архиву через remotezip...")
    with RemoteZip(gcs_url, initial_buffer_size=10_000_000) as zf:
        all_names = zf.namelist()
        # только train/ (в test/ нет цены)
        json_names = [
            n for n in all_names
            if n.endswith("/data.json") and n.startswith("train/")
        ]
        total = len(json_names)
        test_count = sum(
            1 for n in all_names
            if n.endswith("/data.json") and n.startswith("test/")
        )
        print(f"Найдено {total} train JSON + {test_count} test JSON "
              f"(из {len(all_names)} файлов в архиве)")

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        batch = []
        checkpoint_ids = set(loaded_ids)

        try:
            for name in json_names:
                hash_id = _extract_hash_id(name)
                if not hash_id:
                    continue
                if hash_id in checkpoint_ids:
                    stats["skipped"] += 1
                    continue

                try:
                    raw = zf.read(name)
                    data = json.loads(raw)
                    record = _transform(data, hash_id)
                    batch.append(record)
                    checkpoint_ids.add(hash_id)
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    stats["errors"] += 1
                    if stats["errors"] <= 10:
                        print(f"  Ошибка парсинга {name}: {e}")

                # flush
                if len(batch) >= BATCH_SIZE:
                    ins, dups, nop = _flush_batch(cursor, batch)
                    conn.commit()
                    stats["inserted"] += ins
                    stats["skipped"] += dups
                    stats["no_price"] += nop
                    batch.clear()

                # чекпоинт
                stats["processed"] += 1
                if stats["processed"] % CHECKPOINT_EVERY == 0:
                    save_checkpoint("angultiaev", {
                        "loaded_ids": list(checkpoint_ids),
                        "stats": stats,
                    })
                    print(
                        f"  [{stats['processed']}/{total}] "
                        f"вставлено={stats['inserted']}, "
                        f"ошибок={stats['errors']}"
                    )

            if batch:  # остатки
                ins, dups, nop = _flush_batch(cursor, batch)
                conn.commit()
                stats["inserted"] += ins
                stats["skipped"] += dups
                stats["no_price"] += nop

        finally:
            cursor.close()
            conn.close()

    return stats


def _load_via_stream(url: str, auth: tuple, loaded_ids: set) -> dict:
    """Fallback: стримит весь ZIP, обрабатывает только data.json на лету."""
    import requests
    from stream_unzip import stream_unzip

    stats = {"processed": 0, "inserted": 0, "skipped": 0, "no_price": 0, "errors": 0}

    print("Режим stream-unzip: стримим весь архив, фильтруем на лету...")
    print("(Трафик ~162 ГБ, но 0 дискового пространства)")

    gcs_url = _resolve_gcs_url(url, auth)
    resp = requests.get(gcs_url, stream=True, timeout=60)
    resp.raise_for_status()

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    batch = []
    checkpoint_ids = set(loaded_ids)

    try:
        for file_name_bytes, file_size, chunks in stream_unzip(
            resp.iter_content(chunk_size=65536)
        ):
            name = file_name_bytes.decode("utf-8", errors="replace")

            if not name.endswith("/data.json") or not name.startswith("train/"):
                for _ in chunks:  # обязаны прочитать чанки
                    pass
                continue

            hash_id = _extract_hash_id(name)
            if not hash_id:
                for _ in chunks:
                    pass
                continue

            if hash_id in checkpoint_ids:
                for _ in chunks:
                    pass
                stats["skipped"] += 1
                continue

            try:
                raw = b"".join(chunks)
                data = json.loads(raw)
                record = _transform(data, hash_id)
                batch.append(record)
                checkpoint_ids.add(hash_id)
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                stats["errors"] += 1
                if stats["errors"] <= 10:
                    print(f"  Ошибка парсинга {name}: {e}")

            if len(batch) >= BATCH_SIZE:
                ins, skip = _flush_batch(cursor, batch)
                conn.commit()
                stats["inserted"] += ins
                stats["skipped"] += skip
                batch.clear()

            # чекпоинт
            stats["processed"] += 1
            if stats["processed"] % CHECKPOINT_EVERY == 0:
                save_checkpoint("angultiaev", {
                    "loaded_ids": list(checkpoint_ids),
                    "stats": stats,
                })
                print(
                    f"  [{stats['processed']}] "
                    f"вставлено={stats['inserted']}, "
                    f"ошибок={stats['errors']}"
                )

        if batch:  # остатки
            ins, skip = _flush_batch(cursor, batch)
            conn.commit()
            stats["inserted"] += ins
            stats["skipped"] += skip

    finally:
        cursor.close()
        conn.close()
        resp.close()

    return stats


def _get_loaded_count() -> int:
    """Сколько записей этого источника уже в БД."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM flats WHERE source = %s", (SOURCE_NAME,)
        )
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception:
        return 0


def main():
    existing = _get_loaded_count()

    username, key = _get_kaggle_auth()
    auth = (username, key)

    # чекпоинт (если был обрыв)
    checkpoint = load_checkpoint("angultiaev")
    loaded_ids = set(checkpoint.get("loaded_ids", [])) if checkpoint else set()
    if existing and not loaded_ids:
        # уже загружено ранее
        print(f"Источник '{SOURCE_NAME}' уже в БД ({existing} записей).")
        print("(Для перезагрузки: DELETE FROM flats WHERE source = "
              f"'{SOURCE_NAME}')")
        return
    if loaded_ids:
        print(f"Продолжение загрузки: {len(loaded_ids)} в чекпоинте, "
              f"{existing} в БД")

    # пробуем remotezip, fallback на stream-unzip
    try:
        print("=" * 60)
        print(f"Загрузка: {DATASET_HANDLE}")
        print("Стратегия: remotezip (HTTP Range requests)")
        print("=" * 60)
        stats = _load_via_remotezip(DOWNLOAD_URL, auth, loaded_ids)

    except Exception as e:
        print(f"\nremotezip не сработал: {e}")
        print("Переключаемся на stream-unzip (fallback)...\n")
        stats = _load_via_stream(DOWNLOAD_URL, auth, loaded_ids)

    # итоги
    clear_checkpoint("angultiaev")
    print("\n" + "=" * 60)
    print("ИТОГО:")
    print(f"  Обработано JSON:   {stats['processed']}")
    print(f"  Вставлено в БД:    {stats['inserted']}")
    print(f"  Дубликатов:        {stats['skipped']}")
    print(f"  Без цены:          {stats['no_price']}")
    print(f"  Ошибок парсинга:   {stats['errors']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
