import logging
import math
from datetime import datetime

import pandas as pd
from psycopg2.extras import Json, execute_values

from db.connection import get_conn, put_conn

log = logging.getLogger("re")


LISTING_COLUMNS = [
    "cian_id", "url",
    "price", "price_per_m2", "price_type", "mortgage_allowed", "deal_conditions",
    "region", "municipality", "district", "microdistrict", "street", "house", "lat", "lon",
    "metro_stations",
    "rooms", "is_studio", "flat_type", "total_area", "living_area", "kitchen_area",
    "floor", "total_floors", "ceiling_height",
    "renovation", "bathrooms", "balcony", "window_view", "is_apartments",
    "year_built", "building_type", "parking", "passenger_lifts", "cargo_lifts",
    "is_new_building", "developer", "residential_complex", "completion_date",
    "description", "publication_date", "edit_date",
    "seller_type", "seller_user_type", "phone_protected",
    "photos_count", "views_total", "views_today",
]

_DATA_COLS = [c for c in LISTING_COLUMNS if c != "cian_id"]

_BACKFILL_EXCLUDE = {"price", "url", "deal_conditions"}
BACKFILL_COLS = [c for c in _DATA_COLS if c not in _BACKFILL_EXCLUDE]

_COL_CAST = {
    "price_per_m2": "bigint", "lat": "real", "lon": "real",
    "metro_stations": "jsonb", "rooms": "smallint", "is_studio": "boolean",
    "total_area": "real", "living_area": "real", "kitchen_area": "real",
    "floor": "smallint", "total_floors": "smallint", "ceiling_height": "real",
    "is_apartments": "boolean", "year_built": "smallint",
    "passenger_lifts": "smallint", "cargo_lifts": "smallint",
    "is_new_building": "boolean", "phone_protected": "boolean",
    "mortgage_allowed": "boolean", "photos_count": "smallint",
    "views_total": "integer", "views_today": "integer",
}

_ALL_INSERT_COLS = LISTING_COLUMNS + [
    "first_seen_at", "last_seen_at", "updated_at", "consecutive_misses",
]

_ARCHIVE_COLS = LISTING_COLUMNS + [
    "is_active", "first_seen_at", "last_seen_at", "updated_at", "consecutive_misses",
]

_UPSERT_SQL = """
INSERT INTO listings ({cols})
VALUES %s
ON CONFLICT (cian_id) DO UPDATE SET
    {sets},
    last_seen_at = NOW(),
    consecutive_misses = 0,
    updated_at = CASE
        WHEN listings.price IS DISTINCT FROM EXCLUDED.price
          OR listings.total_area IS DISTINCT FROM EXCLUDED.total_area
          OR listings.renovation IS DISTINCT FROM EXCLUDED.renovation
          OR listings.is_active = FALSE
        THEN NOW()
        ELSE listings.updated_at
    END,
    is_active = TRUE
""".format(
    cols=", ".join(_ALL_INSERT_COLS),
    sets=",\n    ".join(f"{c} = EXCLUDED.{c}" for c in _DATA_COLS),
)

_UPSERT_TMPL = "(" + ", ".join(f"%({c})s" for c in _ALL_INSERT_COLS) + ")"


def _clean(value):
    # NaN/Inf/NaT в None, иначе постгрес упадет
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


def build_listing_row(record: dict) -> dict:
    row = {}
    for col in LISTING_COLUMNS:
        val = _clean(record.get(col))
        if col == "metro_stations" and val is not None:
            val = Json(val)
        if col == "is_apartments" and val is None:
            val = _clean(record.get("is_apartment"))
        row[col] = val
    now = datetime.now()
    row["first_seen_at"] = now
    row["last_seen_at"] = now
    row["updated_at"] = now
    row["consecutive_misses"] = 0
    return row


def upsert_listings(rows: list[dict]) -> dict:
    if not rows:
        return {"inserted": 0, "updated": 0, "price_changes": 0}

    cleaned = [build_listing_row(r) for r in rows]
    cleaned = [r for r in cleaned if r.get("price") and r.get("cian_id")]
    if not cleaned:
        return {"inserted": 0, "updated": 0, "price_changes": 0}

    cian_ids = [int(r["cian_id"]) for r in cleaned]

    conn = get_conn()
    try:
        cur = conn.cursor()

        # старые цены для детекции изменений
        old_prices = {}
        for i in range(0, len(cian_ids), 5000):
            chunk = cian_ids[i:i + 5000]
            ph = ",".join(["%s"] * len(chunk))
            cur.execute(
                f"SELECT cian_id, price FROM listings WHERE cian_id IN ({ph})", chunk
            )
            for row in cur:
                old_prices[row[0]] = row[1]

        # upsert
        execute_values(cur, _UPSERT_SQL, cleaned, template=_UPSERT_TMPL)
        total = cur.rowcount

        # пишем в price_history если цена изменилась
        price_rows = []
        for r in cleaned:
            cid = int(r["cian_id"])
            new_p = r["price"]
            old_p = old_prices.get(cid)
            # новый листинг или цена изменилась
            if old_p is None or old_p != new_p:
                price_rows.append((cid, new_p, r.get("price_per_m2"), None))

        # html-история цен из циана, дедуплицируем
        html_cids = [int(r["cian_id"]) for r in rows if r.get("_html_price_history") and r.get("cian_id")]
        existing_ph = set()
        if html_cids:
            for i in range(0, len(html_cids), 5000):
                chunk = html_cids[i:i + 5000]
                ph_q = ",".join(["%s"] * len(chunk))
                cur.execute(
                    f"SELECT cian_id, price FROM price_history WHERE cian_id IN ({ph_q})", chunk
                )
                for row in cur:
                    existing_ph.add((row[0], row[1]))

        html_history_count = 0
        for r in rows:
            cid = r.get("cian_id")
            html_hist = r.get("_html_price_history") or []
            if not cid or not html_hist:
                continue
            cid = int(cid)
            cur_price = r.get("price")
            for entry in html_hist:
                hp = entry.get("price")
                hd = entry.get("date")
                if not hp or hp == cur_price:
                    continue
                if (cid, hp) in existing_ph:
                    continue
                price_rows.append((cid, hp, None, hd))
                existing_ph.add((cid, hp))
                html_history_count += 1

        if price_rows:
            execute_values(
                cur,
                "INSERT INTO price_history (cian_id, price, price_per_m2, recorded_at) VALUES %s",
                [(cid, p, pm, d or datetime.now()) for cid, p, pm, d in price_rows],
                template="(%s, %s, %s, %s)",
            )

        conn.commit()
        cur.close()

        known = old_prices.keys() & set(cian_ids)
        inserted = total - len(known)
        updated = total - inserted
        return {
            "inserted": inserted,
            "updated": updated,
            "price_changes": len(price_rows) - html_history_count,
            "html_history": html_history_count,
        }
    except Exception as e:
        conn.rollback()
        log.error(f"upsert_listings error: {e}")
        return {"inserted": 0, "updated": 0, "price_changes": 0}
    finally:
        put_conn(conn)


def update_offer_fields(rows: list[dict]) -> dict:
    if not rows:
        return {"listings": 0, "archive": 0}

    cleaned = []
    for r in rows:
        cid = r.get("cian_id")
        if not cid:
            continue
        row = {"cian_id": int(cid)}
        for c in BACKFILL_COLS:
            val = _clean(r.get(c))
            if c == "metro_stations" and val is not None:
                val = Json(val)
            row[c] = val
        cleaned.append(row)
    if not cleaned:
        return {"listings": 0, "archive": 0}

    set_clause = ", ".join(
        f"{c} = data.{c}::{_COL_CAST[c]}" if c in _COL_CAST else f"{c} = data.{c}"
        for c in BACKFILL_COLS
    )
    cols_sql = "cian_id, " + ", ".join(BACKFILL_COLS)
    tmpl = "(" + ", ".join(f"%({c})s" for c in (["cian_id"] + BACKFILL_COLS)) + ")"

    conn = get_conn()
    try:
        cur = conn.cursor()
        execute_values(cur, f"""
            UPDATE listings AS t SET {set_clause}
            FROM (VALUES %s) AS data ({cols_sql})
            WHERE t.cian_id = data.cian_id
        """, cleaned, template=tmpl)
        n_listings = cur.rowcount
        execute_values(cur, f"""
            UPDATE listings_archive AS t SET {set_clause}
            FROM (VALUES %s) AS data ({cols_sql})
            WHERE t.cian_id = data.cian_id
        """, cleaned, template=tmpl)
        n_archive = cur.rowcount
        conn.commit()
        cur.close()
        return {"listings": n_listings, "archive": n_archive}
    except Exception as e:
        conn.rollback()
        log.error(f"update_offer_fields error: {e}")
        return {"listings": 0, "archive": 0}
    finally:
        put_conn(conn)


def get_listing_cache() -> dict[int, dict]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT cian_id, last_seen_at, price FROM listings WHERE is_active")
        cache = {
            row[0]: {"last_seen_at": row[1], "price": row[2]}
            for row in cur
        }
        cur.close()
        return cache
    finally:
        put_conn(conn)


def touch_listings(cian_ids: list[int]) -> int:
    if not cian_ids:
        return 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        execute_values(
            cur,
            "UPDATE listings SET last_seen_at = NOW(), consecutive_misses = 0 "
            "WHERE cian_id IN (VALUES %s)",
            [(cid,) for cid in cian_ids],
        )
        count = cur.rowcount
        conn.commit()
        cur.close()
        return count
    except Exception as e:
        conn.rollback()
        log.error(f"touch_listings error: {e}")
        return 0
    finally:
        put_conn(conn)


def run_deactivation(run_start: datetime) -> dict:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE listings SET consecutive_misses = consecutive_misses + 1 "
            "WHERE is_active AND last_seen_at < %s",
            (run_start,),
        )
        missed = cur.rowcount
        cur.execute(
            "UPDATE listings SET is_active = FALSE "
            "WHERE consecutive_misses >= 3 AND is_active"
        )
        deactivated = cur.rowcount
        conn.commit()
        cur.close()
        log.info(f"deactivation: {missed} missed, {deactivated} deactivated")
        return {"missed": missed, "deactivated": deactivated}
    except Exception as e:
        conn.rollback()
        log.error(f"deactivation error: {e}")
        return {"missed": 0, "deactivated": 0}
    finally:
        put_conn(conn)


def insert_daily_snapshot() -> int:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cols = ", ".join(_ARCHIVE_COLS)
        cur.execute(f"""
            INSERT INTO listings_archive ({cols}, snapshot_date)
            SELECT {cols}, CURRENT_DATE FROM listings WHERE is_active
            ON CONFLICT (cian_id, snapshot_date) DO NOTHING
        """)
        count = cur.rowcount
        conn.commit()
        cur.close()
        log.info(f"daily snapshot: {count} listings archived for {datetime.now().date()}")
        return count
    except Exception as e:
        conn.rollback()
        log.error(f"snapshot error: {e}")
        return 0
    finally:
        put_conn(conn)


def archive_inactive() -> int:
    """переносим деактивированные объявления в архив и удаляем из listings"""
    conn = get_conn()
    try:
        cur = conn.cursor()
        # snapshot_date = дата последнего обновления (когда объявление было ещё живым)
        cols = ", ".join(_ARCHIVE_COLS)
        cur.execute(f"""
            INSERT INTO listings_archive ({cols}, snapshot_date)
            SELECT {cols}, updated_at::date FROM listings WHERE NOT is_active
            ON CONFLICT (cian_id, snapshot_date) DO NOTHING
        """)
        archived = cur.rowcount

        cur.execute("DELETE FROM listings WHERE NOT is_active")
        deleted = cur.rowcount

        conn.commit()
        cur.close()
        log.info(f"archive_inactive: {archived} archived, {deleted} deleted from listings")
        return deleted
    except Exception as e:
        conn.rollback()
        log.error(f"archive_inactive error: {e}")
        return 0
    finally:
        put_conn(conn)


# legacy, kaggle loader

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

_LEGACY_SQL = "INSERT INTO kaggle_flats ({cols}) VALUES %s ON CONFLICT (url, source, price) DO NOTHING".format(
    cols=", ".join(COLUMNS),
)
_LEGACY_TMPL = "(" + ", ".join(f"%({c})s" for c in COLUMNS) + ")"

# алиас для kaggle loaders которые импортируют INSERT_SQL
INSERT_SQL = _LEGACY_SQL


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
        execute_values(cur, _LEGACY_SQL, cleaned, template=_LEGACY_TMPL)
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
            f"SELECT DISTINCT url FROM kaggle_flats WHERE source IN ({ph})", sources
        )
        urls = {row[0] for row in cur.fetchall()}
        cur.close()
        return urls
    finally:
        put_conn(conn)
