import logging
import math

import pandas as pd
from psycopg2.extras import Json, execute_values

from pipeline.core.connection import get_conn, put_conn

log = logging.getLogger("re")


def _clean(value):
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


COLUMNS = [
    "url", "source", "price", "price_per_m2", "discount_pct", "deal_conditions",
    "region", "municipality", "district", "microdistrict", "street", "house", "lat", "lon",
    "metro_stations", "transport_score",
    "rooms", "total_area", "living_area", "kitchen_area",
    "floor", "total_floors", "ceiling_height",
    "renovation", "bathrooms", "balcony", "window_view", "is_apartments",
    "year_built", "building_type", "parking", "elevators",
    "is_new_building", "developer", "residential_complex", "completion_date",
    "description", "publication_date",
]

INSERT_SQL = "INSERT INTO raw.kaggle_flats ({cols}) VALUES %s ON CONFLICT (url, source, price) DO NOTHING".format(
    cols=", ".join(COLUMNS),
)
_TMPL = "(" + ", ".join(f"%({c})s" for c in COLUMNS) + ")"


def build_row(record: dict) -> dict:
    row = {}
    for col in COLUMNS:
        val = _clean(record.get(col))
        if col == "metro_stations" and val is not None:
            val = Json(val)
        if col == "is_apartments" and val is None:
            val = _clean(record.get("is_apartment"))
        row[col] = val
    return row


def save_rows(rows: list[dict]) -> int:
    if not rows:
        return 0
    cleaned = [build_row(r) for r in rows]
    cleaned = [r for r in cleaned if r.get("price")]
    if not cleaned:
        return 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        execute_values(cur, INSERT_SQL, cleaned, template=_TMPL)
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


def get_cached_urls(sources: list[str]) -> set[str]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        ph = ",".join(["%s"] * len(sources))
        cur.execute(
            f"SELECT DISTINCT url FROM raw.kaggle_flats WHERE source IN ({ph})", sources
        )
        urls = {row[0] for row in cur.fetchall()}
        cur.close()
        return urls
    finally:
        put_conn(conn)
