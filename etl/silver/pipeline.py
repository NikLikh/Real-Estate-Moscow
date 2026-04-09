import logging

import psycopg2

from config.settings import DB_CONFIG

log = logging.getLogger("re")

SILVER_COLUMNS = [
    "cian_id", "source", "url",
    "dedup_group_id", "is_primary", "group_size",
    "group_min_price", "group_max_price", "group_seller_types",
    "price", "price_per_m2",
    "city", "region", "municipality", "district", "microdistrict",
    "street", "house", "lat", "lon",
    "okrug", "raion", "nearest_metro", "metro_distance_m",
    "metro_walk_min", "dist_to_center_km", "metro_stations",
    "rooms", "total_area", "living_area", "kitchen_area",
    "floor", "total_floors", "ceiling_height",
    "floor_ratio", "living_ratio", "kitchen_ratio",
    "building_type", "year_built", "year_built_source", "building_era",
    "renovation", "bathrooms", "balcony", "window_view", "parking",
    "is_apartments", "is_new_building", "developer", "residential_complex",
    "completion_date", "completion_year", "stage",
    "pub_date AS publication_date", "pub_year", "pub_month", "pub_quarter", "date_source",
    "seller_type", "is_active", "first_seen_at", "last_seen_at",
    "has_coords", "has_year_built", "has_pub_date", "data_quality_score",
]


def run_silver_etl():
    """Silver ETL полностью в PostgreSQL, без Spark"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    from etl.silver.extract import extract_to_staging
    from etl.silver.clean import clean
    from etl.silver.normalize import normalize
    from etl.silver.crossfill import crossfill
    from etl.silver.geo_enrich import geo_enrich
    from etl.silver.impute import impute
    from etl.silver.dedup import dedup
    from etl.silver.features import compute_features

    log.info("=== extract ===")
    extract_to_staging(cur)

    log.info("=== clean ===")
    clean(cur)

    log.info("=== normalize ===")
    normalize(cur)

    log.info("=== crossfill ===")
    crossfill(cur)

    log.info("=== geo_enrich ===")
    geo_enrich(cur)

    log.info("=== impute ===")
    impute(cur)

    log.info("=== dedup ===")
    dedup(cur)

    log.info("=== features ===")
    compute_features(cur)

    # финальная запись в silver_listings
    log.info("=== write silver_listings ===")
    cols = ", ".join(SILVER_COLUMNS)
    cur.execute("TRUNCATE silver_listings")
    cur.execute(f"""
        INSERT INTO silver_listings ({', '.join(c.split(' AS ')[-1] for c in SILVER_COLUMNS)})
        SELECT {cols} FROM silver_staging
        WHERE price_per_m2 IS NOT NULL
    """)
    conn.commit()

    cur.execute("SELECT count(*) FROM silver_listings")
    count = cur.fetchone()[0]
    log.info(f"silver_listings: {count} rows")

    # staging больше не нужен
    cur.execute("DROP TABLE IF EXISTS silver_staging")
    conn.commit()

    cur.close()
    conn.close()
    return count
