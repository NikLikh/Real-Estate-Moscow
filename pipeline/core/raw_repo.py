import logging

from psycopg2.extras import Json, execute_values

from pipeline.core.connection import get_conn, put_conn

log = logging.getLogger("re")

OBS_COLS = [
    "cian_id", "run_id", "payload", "url", "deal_type",
    "price", "price_type", "currency", "mortgage_allowed", "deal_conditions", "price_per_m2",
    "region", "municipality", "district", "microdistrict", "street", "house", "house_id",
    "lat", "lon", "metro_stations",
    "rooms", "is_studio", "flat_type", "total_area", "living_area", "kitchen_area",
    "floor", "total_floors", "ceiling_height",
    "renovation", "bathrooms", "balcony", "window_view", "parking",
    "is_apartments", "year_built", "building_type", "passenger_lifts", "cargo_lifts",
    "is_new_building", "nb_house_id", "developer", "residential_complex", "completion_date",
    "description", "descr_minhash", "publication_date", "edit_date",
    "seller_type", "seller_user_type", "phone_protected",
    "photos_count", "views_total", "views_today",
    "seller_is_owner", "status", "cian_user_id", "is_penthouse", "room_type", "demolished_in_renovation",
    "railways", "highways", "is_emergency", "year_release", "has_playground", "has_sportsground",
    "house_material_type", "house_heat_supply_type", "house_gas_supply_type", "house_overlap_type",
    "house_overhaul_fund_type", "flat_count", "entrances", "series_name", "chute_count",
    "has_furniture", "has_ramp", "all_rooms_area",
    "from_developer", "user_trust_level", "is_agent", "is_builder", "agency_name",
    "deposit", "agent_fee", "client_fee", "prepay_months", "lease_term_type", "payment_period",
    "utilities_included", "utilities_price", "beds_count", "pets_allowed", "children_allowed",
    "has_fridge", "has_washer", "has_dishwasher", "has_conditioner", "has_tv", "has_internet",
]

_JSON_COLS = {"payload", "metro_stations", "railways", "highways"}


def insert_observations(rows, run_id=None):
    if not rows:
        return 0
    try:
        return _insert_batch(rows, run_id)
    except Exception as e:
        if len(rows) == 1:
            log.warning(f"[DB] строка cian_id={rows[0].get('cian_id')} отброшена: {e}")
            return 0
        mid = len(rows) // 2
        return insert_observations(rows[:mid], run_id) + insert_observations(rows[mid:], run_id)


def _insert_batch(rows, run_id=None):
    values = []
    for r in rows:
        row = []
        for c in OBS_COLS:
            v = run_id if c == "run_id" else r.get(c)
            row.append(Json(v) if c in _JSON_COLS and v is not None else v)
        values.append(row)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO raw.cian_observations ({', '.join(OBS_COLS)}) VALUES %s",
                values,
                page_size=1000,
            )
        conn.commit()
        return len(values)
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)


RUN_COLS = [
    "run_id", "minutes", "plan_offers", "plan_filters", "cards", "presence",
    "parsed", "saved", "repriced", "empty_pages", "incomplete", "pages_lost",
    "captchas", "net_errors", "waf_blocks", "restarts", "pool_slots", "pool_alive",
]


def insert_run_stats(row):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO raw.scrape_runs ({', '.join(RUN_COLS)}) VALUES ({', '.join(['%s'] * len(RUN_COLS))})"
                " ON CONFLICT (run_id) DO NOTHING",
                [row.get(c) for c in RUN_COLS],
            )
        conn.commit()
    finally:
        put_conn(conn)


def get_current_state():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (cian_id) cian_id, price, scraped_at
                FROM raw.cian_observations
                ORDER BY cian_id, scraped_at DESC
            """)
            state = {row[0]: {"price": row[1], "last_seen_at": row[2]} for row in cur}
        conn.rollback()
        return state
    finally:
        put_conn(conn)


_BANDS_SQL = """
with wanted as (
    select * from jsonb_to_recordset(%s::jsonb)
        as x(region text, rkey text, rks text[], isnew boolean, k int)
),
prices as (
    select region, is_new_building as isnew, price,
           case when flat_type = 'studio' then 'studio'
                when rooms >= 5 then '5+'
                else rooms::text end as rk
    from marts.current_listings
    where price > 0
)
select w.rkey,
       percentile_disc((select array_agg(i::float8 / w.k) from generate_series(1, w.k - 1) i))
           within group (order by p.price),
       count(*)
from wanted w
join prices p on p.region = w.region
             and p.rk = any(w.rks)
             and (w.isnew is null or p.isnew = w.isnew)
group by w.rkey, w.k
"""


def fetch_price_bands(segments):
    if not segments:
        return {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(_BANDS_SQL, (Json(segments),))
            bands = {row[0]: (row[1], row[2]) for row in cur}
        conn.rollback()
        return bands
    finally:
        put_conn(conn)


def fetch_region_names():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("select distinct region from marts.current_listings where region is not null")
            names = [row[0] for row in cur]
        conn.rollback()
        return names
    finally:
        put_conn(conn)
