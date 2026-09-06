
import json
import logging
import os
import re
from pathlib import Path

from pipeline.core.connection import get_conn, put_conn
from pipeline.kaggle.repository import INSERT_SQL, build_row
from pipeline.cian.runtime import clear_checkpoint, load_checkpoint, save_checkpoint

log = logging.getLogger("re")

DATASET_HANDLE = "angultiaev/flat-sale-m24ml"
DOWNLOAD_URL = (
    "https://www.kaggle.com/api/v1/datasets/download/angultiaev/flat-sale-m24ml"
)
SOURCE_NAME = "kaggle_angultiaev"
BATCH_SIZE = 500
CHECKPOINT_EVERY = 1000


def _get_kaggle_auth() -> tuple[str, str]:
    username = os.environ.get("KAGGLE_USERNAME")
    key = os.environ.get("KAGGLE_KEY")
    if username and key:
        return username, key

    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    if kaggle_json.exists():
        with open(kaggle_json, encoding="utf-8") as f:
            creds = json.load(f)
        return creds["username"], creds["key"]

    print("Kaggle API credentials not found.")
    print("Get them: kaggle.com -> Settings -> API -> Create New Token")
    username = input("  Kaggle username: ").strip()
    key = input("  Kaggle API key: ").strip()
    if not username or not key:
        raise ValueError("credentials cannot be empty")

    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(parents=True, exist_ok=True)
    kaggle_json = kaggle_dir / "kaggle.json"
    with open(kaggle_json, "w", encoding="utf-8") as f:
        json.dump({"username": username, "key": key}, f)
    log.info(f"saved to {kaggle_json}")

    return username, key


def _parse_price(raw):
    if not raw:
        return None
    digits = re.sub(r"[^\d]", "", raw)
    return int(digits) if digits else None


def _parse_area(raw):
    if not raw:
        return None
    m = re.search(r"([\d]+[.,]?\d*)", raw)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def _parse_floor(raw):
    if not raw:
        return None, None
    m = re.search(r"(\d+)\s*из\s*(\d+)", raw)
    if m:
        return int(m.group(1)), int(m.group(2))
    m2 = re.search(r"(\d+)", raw)
    return (int(m2.group(1)), None) if m2 else (None, None)


def _parse_rooms(title):
    if not title:
        return None
    t = title.lower()
    if "студи" in t:
        return 0
    m = re.search(r"(\d+)\s*-?\s*комн", t)
    return int(m.group(1)) if m else None


def _parse_address(raw):
    result = {
        "city": None,
        "region": None,
        "district": None,
        "street": None,
        "house_number": None,
    }
    if not raw:
        return result

    parts = [p.strip() for p in raw.split(",")]
    if not parts:
        return result

    result["city"] = parts[0]

    remaining = parts[1:]
    for part in remaining:
        if re.search(r"р-н|район", part, re.IGNORECASE):
            result["district"] = part
        elif re.match(r"^[А-ЯЁA-Z]{2,5}$", part.strip()):
            result["region"] = part
        elif re.search(
            r"ул\.|улица|пер\.|переулок|просп|пр-т|ш\.|шоссе|б-р|бульвар|наб\.|набережная|пр-д|проезд",
            part,
            re.IGNORECASE,
        ):
            result["street"] = part
        elif (
            re.match(r"^\d", part.strip())
            and result["street"]
            and not result["house_number"]
        ):
            result["house_number"] = part

    if not result["street"] and len(remaining) >= 2:
        for part in remaining:
            if part != result.get("region") and part != result.get("district"):
                if not result["street"]:
                    result["street"] = part
                elif not result["house_number"]:
                    result["house_number"] = part

    return result


def _parse_metro(stations):
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


def _parse_year(raw):
    if not raw:
        return None
    m = re.search(r"(\d{4})", str(raw))
    return int(m.group(1)) if m else None


def _transform(data: dict, hash_id: str) -> dict:
    summary = data.get("offer_summary") or {}
    details = data.get("offer_details") or {}

    floor, total_floors = _parse_floor(summary.get("Этаж"))
    addr = _parse_address(data.get("address"))

    housing_type = summary.get("Тип жилья", "")
    is_new = (
        True
        if "новостройка" in housing_type.lower()
        else (False if housing_type else None)
    )

    metro = _parse_metro(data.get("nearest_stations"))

    return {
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
        "ceiling_height": _parse_area(summary.get("Высота потолков")),
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


def _extract_hash_id(zip_path: str):
    parts = zip_path.replace("\\", "/").split("/")
    if len(parts) >= 2 and parts[-1] == "data.json":
        return parts[-2]
    return None


def _flush_batch(cursor, batch: list[dict]) -> tuple[int, int, int]:
    if not batch:
        return 0, 0, 0

    rows = []
    no_price = 0
    for record in batch:
        row = build_row(record)
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
            log.error(f"insert error {row.get('url', '?')}: {e}")

    return inserted, len(rows) - inserted, no_price


def _resolve_gcs_url(kaggle_url: str, auth: tuple) -> str:
    import requests

    resp = requests.get(
        kaggle_url, auth=auth, stream=True, timeout=60, allow_redirects=True
    )
    resp.raise_for_status()
    gcs_url = resp.url
    resp.close()
    log.info("GCS URL obtained")
    return gcs_url


def _load_via_remotezip(url: str, auth: tuple, loaded_ids: set) -> dict:
    from remotezip import RemoteZip

    stats = {"processed": 0, "inserted": 0, "skipped": 0, "no_price": 0, "errors": 0}

    gcs_url = _resolve_gcs_url(url, auth)

    log.info("connecting via remotezip...")
    with RemoteZip(gcs_url, initial_buffer_size=10_000_000) as zf:
        all_names = zf.namelist()
        json_names = [
            n for n in all_names if n.endswith("/data.json") and n.startswith("train/")
        ]
        total = len(json_names)
        test_count = sum(
            1 for n in all_names if n.endswith("/data.json") and n.startswith("test/")
        )
        log.info(
            f"found {total} train JSON + {test_count} test JSON "
            f"(of {len(all_names)} files in archive)"
        )

        conn = get_conn()
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
                        log.warning(f"parse error {name}: {e}")

                if len(batch) >= BATCH_SIZE:
                    ins, dups, nop = _flush_batch(cursor, batch)
                    conn.commit()
                    stats["inserted"] += ins
                    stats["skipped"] += dups
                    stats["no_price"] += nop
                    batch.clear()

                stats["processed"] += 1
                if stats["processed"] % CHECKPOINT_EVERY == 0:
                    save_checkpoint(
                        "angultiaev",
                        {
                            "loaded_ids": list(checkpoint_ids),
                            "stats": stats,
                        },
                    )
                    log.info(
                        f"[{stats['processed']}/{total}] "
                        f"inserted={stats['inserted']}, errors={stats['errors']}"
                    )

            if batch:
                ins, dups, nop = _flush_batch(cursor, batch)
                conn.commit()
                stats["inserted"] += ins
                stats["skipped"] += dups
                stats["no_price"] += nop

        finally:
            cursor.close()
            put_conn(conn)

    return stats


def _load_via_stream(url: str, auth: tuple, loaded_ids: set) -> dict:
    import requests
    from stream_unzip import stream_unzip

    stats = {"processed": 0, "inserted": 0, "skipped": 0, "no_price": 0, "errors": 0}

    log.info("stream-unzip mode: streaming full archive, filtering on the fly...")

    gcs_url = _resolve_gcs_url(url, auth)
    resp = requests.get(gcs_url, stream=True, timeout=60)
    resp.raise_for_status()

    conn = get_conn()
    cursor = conn.cursor()
    batch = []
    checkpoint_ids = set(loaded_ids)

    try:
        for file_name_bytes, file_size, chunks in stream_unzip(
            resp.iter_content(chunk_size=65536)
        ):
            name = file_name_bytes.decode("utf-8", errors="replace")

            if not name.endswith("/data.json") or not name.startswith("train/"):
                for _ in chunks:
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
                    log.warning(f"parse error {name}: {e}")

            if len(batch) >= BATCH_SIZE:
                ins, dups, nop = _flush_batch(cursor, batch)
                conn.commit()
                stats["inserted"] += ins
                stats["skipped"] += dups
                stats["no_price"] += nop
                batch.clear()

            stats["processed"] += 1
            if stats["processed"] % CHECKPOINT_EVERY == 0:
                save_checkpoint(
                    "angultiaev",
                    {
                        "loaded_ids": list(checkpoint_ids),
                        "stats": stats,
                    },
                )
                log.info(
                    f"[{stats['processed']}] "
                    f"inserted={stats['inserted']}, errors={stats['errors']}"
                )

        if batch:
            ins, dups, nop = _flush_batch(cursor, batch)
            conn.commit()
            stats["inserted"] += ins
            stats["skipped"] += dups
            stats["no_price"] += nop

    finally:
        cursor.close()
        put_conn(conn)
        resp.close()

    return stats


def _get_loaded_count() -> int:
    try:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM raw.kaggle_flats WHERE source = %s", (SOURCE_NAME,))
            count = cur.fetchone()[0]
            cur.close()
            return count
        finally:
            put_conn(conn)
    except Exception:
        return 0


def main():
    existing = _get_loaded_count()

    username, key = _get_kaggle_auth()
    auth = (username, key)

    checkpoint = load_checkpoint("angultiaev")
    loaded_ids = set(checkpoint.get("loaded_ids", [])) if checkpoint else set()
    if existing and not loaded_ids:
        log.info(
            f"source '{SOURCE_NAME}' already in DB ({existing} rows). "
            f"To reload: DELETE FROM raw.kaggle_flats WHERE source = '{SOURCE_NAME}'"
        )
        return
    if loaded_ids:
        log.info(f"resuming: {len(loaded_ids)} in checkpoint, {existing} in DB")

    try:
        log.info(f"loading: {DATASET_HANDLE} (remotezip)")
        stats = _load_via_remotezip(DOWNLOAD_URL, auth, loaded_ids)
    except Exception as e:
        log.warning(f"remotezip failed: {e}, switching to stream-unzip...")
        stats = _load_via_stream(DOWNLOAD_URL, auth, loaded_ids)

    clear_checkpoint("angultiaev")
    log.info(
        f"done: processed={stats['processed']} inserted={stats['inserted']} "
        f"skipped={stats['skipped']} no_price={stats['no_price']} "
        f"errors={stats['errors']}"
    )


if __name__ == "__main__":
    main()
