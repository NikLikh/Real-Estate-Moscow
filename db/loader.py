import json
import math
import sys

import pandas as pd
import psycopg2
from psycopg2.extras import Json

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "real_estate",
    "user": "user",
    "password": "password",
}

COLUMNS = [
    "url", "source", "price", "price_per_m2", "discount_pct",
    "deal_conditions", "city", "region", "district", "street",
    "house_number", "lat", "lon", "metro_stations", "transport_score",
    "rooms", "total_area", "living_area", "kitchen_area", "floor",
    "total_floors", "ceiling_height", "renovation", "bathrooms",
    "balcony", "window_view", "is_apartments", "year_built",
    "building_type", "parking", "elevators", "is_new_building",
    "developer", "residential_complex", "completion_date",
    "description", "publication_date",
]

INSERT_SQL = """
    INSERT INTO flats ({columns})
    VALUES ({placeholders})
    ON CONFLICT (url, source, price) DO NOTHING
""".format(
    columns=", ".join(COLUMNS),
    placeholders=", ".join(f"%({col})s" for col in COLUMNS),
)


def _clean_value(value):
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _build_row(record: dict) -> dict:
    row = {}
    for col in COLUMNS:
        value = _clean_value(record.get(col))
        if col == "metro_stations" and value is not None:
            value = Json(value)
        if col == "is_apartments" and value is None:
            value = _clean_value(record.get("is_apartment"))
        row[col] = value
    return row


def get_cached_urls(sources: list[str]) -> set[str]:
    """Достает URL уже спарсенных квартир из БД."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    placeholders = ",".join(["%s"] * len(sources))
    cur.execute(f"SELECT DISTINCT url FROM flats WHERE source IN ({placeholders})", sources)
    urls = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return urls


def save_rows(rows: list[dict]) -> int:
    """Вставляет записи в PG. Возвращает кол-во вставленных."""
    if not rows:
        return 0

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    saved = 0

    for row_data in rows:
        row = _build_row(row_data)
        if row.get("price") is None:
            continue
        try:
            cur.execute(INSERT_SQL, row)
            saved += cur.rowcount
        except Exception as e:
            conn.rollback()
            print(f"  Ошибка записи в БД: {e}")

    conn.commit()
    cur.close()
    conn.close()
    return saved


def load_json_to_pg(json_path: str):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    inserted, skipped, invalid = 0, 0, 0

    for record in data:
        row = _build_row(record)
        if row.get("price") is None:
            invalid += 1
            continue
        cursor.execute(INSERT_SQL, row)
        inserted += cursor.rowcount
        if cursor.rowcount == 0:
            skipped += 1

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Загружено {inserted}, дубликатов {skipped}, невалидных {invalid}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python -m db.loader <путь к JSON>")
        sys.exit(1)
    load_json_to_pg(sys.argv[1])
