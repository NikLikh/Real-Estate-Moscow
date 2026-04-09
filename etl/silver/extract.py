import logging

import psycopg2

from config.settings import DB_CONFIG

log = logging.getLogger("re")


def extract_to_staging(cur):
    """создаем silver_staging из listings + kaggle_flats с единой схемой"""
    cur.execute("DROP TABLE IF EXISTS silver_staging")

    # общие колонки обеих таблиц + приведение house_number -> house, добавление source
    cur.execute(r"""
        CREATE TABLE silver_staging AS

        SELECT
            cian_id, 'cian' AS source, url,
            price, price_per_m2,
            NULL::text AS city, region, NULL::text AS municipality,
            district, NULL::text AS microdistrict,
            street, house,
            lat::double precision, lon::double precision,
            metro_stations::text AS metro_stations,
            rooms, total_area, living_area, kitchen_area,
            floor, total_floors, ceiling_height,
            renovation, bathrooms, balcony, window_view,
            is_apartments, year_built, building_type, parking,
            is_new_building, developer, residential_complex,
            completion_date, publication_date,
            seller_type, is_active,
            first_seen_at, last_seen_at
        FROM listings

        UNION ALL

        SELECT
            -- извлекаем cian_id из url если это ссылка на cian
            (regexp_match(url, '/flat/(\d+)'))[1]::bigint AS cian_id,
            source, url,
            price, price_per_m2,
            city, region, NULL::text AS municipality,
            district, NULL::text AS microdistrict,
            street, house_number AS house,
            lat::double precision, lon::double precision,
            metro_stations::text AS metro_stations,
            rooms, total_area, living_area, kitchen_area,
            floor, total_floors, ceiling_height,
            renovation, bathrooms, balcony, window_view,
            is_apartments, year_built, building_type, parking,
            is_new_building, developer, residential_complex,
            completion_date, publication_date,
            NULL::text AS seller_type, NULL::boolean AS is_active,
            NULL::timestamptz AS first_seen_at, NULL::timestamptz AS last_seen_at
        FROM kaggle_flats
    """)
    cur.connection.commit()

    cur.execute("SELECT count(*) FROM silver_staging")
    n = cur.fetchone()[0]
    log.info(f"extract: silver_staging = {n} rows")
    return n
