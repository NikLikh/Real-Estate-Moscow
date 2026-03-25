"""
Загрузчик данных из JSON в PostgreSQL.

Запуск:
    python -m db.loader support_files/cian_offers.json
    python -m db.loader support_files/domrf_offers.json
"""

import json
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import Json

# Подключение к PG
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "real_estate",
    "user": "user",
    "password": "password",
}

# Колонки таблицы flats
COLUMNS = [
    "url",
    "source",
    "price",
    "price_per_m2",
    "discount_pct",
    "deal_conditions",
    "city",
    "region",
    "district",
    "street",
    "house_number",
    "lat",
    "lon",
    "metro_stations",
    "transport_score",
    "rooms",
    "total_area",
    "living_area",
    "kitchen_area",
    "floor",
    "total_floors",
    "ceiling_height",
    "renovation",
    "bathrooms",
    "balcony",
    "window_view",
    "is_apartments",
    "year_built",
    "building_type",
    "parking",
    "elevators",
    "is_new_building",
    "developer",
    "residential_complex",
    "completion_date",
    "description",
    "publication_date",
]

# SQL для вставки
INSERT_SQL = """
    INSERT INTO flats ({columns})
    VALUES ({placeholders})
    ON CONFLICT (url, source, price) DO NOTHING
""".format(
    columns=", ".join(COLUMNS),
    placeholders=", ".join(f"%({col})s" for col in COLUMNS),
)


def _build_row(record: dict) -> dict:
    """
    Маппит JSON-запись на колонки таблицы flats
    Поля, которых нет в record, получают None
    metro_stations оборачивается в Json() для JSONB
    """
    row = {}
    for col in COLUMNS:
        value = record.get(col)

        if col == "metro_stations" and value is not None:
            value = Json(value)

        if col == "is_apartments" and value is None:
            value = record.get("is_apartment")

        row[col] = value

    return row


def load_json_to_pg(json_path: str):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    inserted, skipped, invalid = 0, 0, 0

    for record in data:
        row = _build_row(record)

        # Пропускаем записи без цены
        if row.get("price") is None:
            invalid += 1
            continue

        cursor.execute(INSERT_SQL, row)
        inserted += cursor.rowcount
        if cursor.rowcount == 0:
            skipped += 1

    conn.commit()

    print(f"Загружено {inserted}, пропущено дубликатов {skipped}, невалидных {invalid}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python -m db.loader <путь к JSON>")
        sys.exit(1)

    load_json_to_pg(sys.argv[1])
