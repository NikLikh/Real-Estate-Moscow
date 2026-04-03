import json
import logging
import math
import sys

import pandas as pd
from psycopg2.extras import Json, execute_values

from db.connection import get_conn, put_conn

log = logging.getLogger("re")

# порядок колонок = порядок в INSERT, должен совпадать с init.sql
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

# price в unique key, потому что одна квартира может менять цену
INSERT_SQL = """
    INSERT INTO flats ({columns})
    VALUES ({placeholders})
    ON CONFLICT (url, source, price) DO NOTHING
""".format(
    columns=", ".join(COLUMNS),
    placeholders=", ".join(f"%({col})s" for col in COLUMNS),
)

BULK_INSERT_SQL = "INSERT INTO flats ({columns}) VALUES %s ON CONFLICT (url, source, price) DO NOTHING".format(
    columns=", ".join(COLUMNS),
)

BULK_TEMPLATE = "(" + ", ".join(f"%({col})s" for col in COLUMNS) + ")"


def _clean_value(value):
    # NaN/Inf/NaT -> None, иначе постгрес упадет
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


def build_row(record: dict) -> dict:
    row = {}
    for col in COLUMNS:
        value = _clean_value(record.get(col))
        if col == "metro_stations" and value is not None:
            value = Json(value)
        if col == "is_apartments" and value is None:
            value = _clean_value(record.get("is_apartment"))  # разные источники пишут по-разному
        row[col] = value
    return row


def get_cached_urls(sources: list[str]) -> set[str]:
    # url-ы которые уже в БД, чтобы не парсить повторно
    conn = get_conn()
    try:
        cur = conn.cursor()
        placeholders = ",".join(["%s"] * len(sources))
        cur.execute(
            f"SELECT DISTINCT url FROM flats WHERE source IN ({placeholders})",
            sources,
        )
        urls = {row[0] for row in cur.fetchall()}
        cur.close()
        return urls
    finally:
        put_conn(conn)


def save_rows(rows: list[dict]) -> int:
    if not rows:
        return 0

    cleaned = [build_row(r) for r in rows]
    cleaned = [r for r in cleaned if r.get("price")]  # без цены нет смысла хранить
    if not cleaned:
        return 0

    conn = get_conn()
    try:
        cur = conn.cursor()
        execute_values(cur, BULK_INSERT_SQL, cleaned, template=BULK_TEMPLATE)
        saved = cur.rowcount
        conn.commit()
        cur.close()
        return saved
    except Exception as e:
        conn.rollback()
        log.error(f"db error: {e}")
        return 0
    finally:
        put_conn(conn)


def load_json_to_pg(json_path: str):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = get_conn()
    try:
        cur = conn.cursor()
        inserted, skipped, invalid = 0, 0, 0

        for record in data:
            row = build_row(record)
            if row.get("price") is None:
                invalid += 1
                continue
            cur.execute(INSERT_SQL, row)
            inserted += cur.rowcount
            if cur.rowcount == 0:
                skipped += 1

        conn.commit()
        cur.close()
        log.info(f"loaded {inserted}, duplicates {skipped}, invalid {invalid}")
    finally:
        put_conn(conn)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("python -m db.repository <path to JSON>")
        sys.exit(1)
    load_json_to_pg(sys.argv[1])
