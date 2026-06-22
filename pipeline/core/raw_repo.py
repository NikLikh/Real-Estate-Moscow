from psycopg2.extras import Json, execute_values

from pipeline.core.connection import get_conn, put_conn

OBS_COLS = [
    "cian_id", "run_id", "payload", "url",
    "price", "price_type", "mortgage_allowed", "deal_conditions", "price_per_m2",
    "region", "municipality", "district", "microdistrict", "street", "house",
    "lat", "lon", "metro_stations",
    "rooms", "is_studio", "flat_type", "total_area", "living_area", "kitchen_area",
    "floor", "total_floors", "ceiling_height",
    "renovation", "bathrooms", "balcony", "window_view", "parking",
    "is_apartments", "year_built", "building_type", "passenger_lifts", "cargo_lifts",
    "is_new_building", "developer", "residential_complex", "completion_date",
    "description", "publication_date", "edit_date",
    "seller_type", "seller_user_type", "phone_protected",
    "photos_count", "views_total", "views_today",
    "seller_is_owner", "status", "cian_user_id", "is_penthouse", "room_type", "demolished_in_renovation",
]

_JSON_COLS = {"payload", "metro_stations"}


def insert_observations(rows, run_id=None):
    if not rows:
        return 0
    values = []
    for r in rows:
        row = []
        for c in OBS_COLS:
            v = run_id if c == "run_id" else r.get(c)
            row.append(Json(v) if c in _JSON_COLS and v is not None else v)
        values.append(row)
    conn = get_conn()
    cur = conn.cursor()
    execute_values(
        cur,
        f"INSERT INTO raw.cian_observations ({', '.join(OBS_COLS)}) VALUES %s",
        values,
    )
    n = cur.rowcount
    conn.commit()
    cur.close()
    put_conn(conn)
    return n


def get_current_state():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ON (cian_id) cian_id, price, scraped_at
        FROM raw.cian_observations
        ORDER BY cian_id, scraped_at DESC
    """)
    state = {row[0]: {"price": row[1], "last_seen_at": row[2]} for row in cur}
    cur.close()
    put_conn(conn)
    return state
