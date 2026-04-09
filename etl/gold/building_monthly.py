import logging

log = logging.getLogger("re")


def build_building_monthly(cur):
    """помесячная медиана price_per_m2 по зданиям (lat4, lon4)"""
    cur.execute("TRUNCATE gold_building_monthly")
    cur.execute("""
        INSERT INTO gold_building_monthly (
            building_id, lat4, lon4, pub_month,
            med_ppm2, avg_ppm2, min_ppm2, max_ppm2, n_obs, n_sources,
            okrug, raion, building_type, building_era,
            is_new_building, total_floors, year_built, nearest_metro
        )
        SELECT
            hashtext(ROUND(lat::numeric, 4)::text || ',' || ROUND(lon::numeric, 4)::text)::bigint AS building_id,
            ROUND(lat::numeric, 4) AS lat4,
            ROUND(lon::numeric, 4) AS lon4,
            pub_month,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2)::bigint AS med_ppm2,
            AVG(price_per_m2)::bigint AS avg_ppm2,
            MIN(price_per_m2) AS min_ppm2,
            MAX(price_per_m2) AS max_ppm2,
            COUNT(*) AS n_obs,
            COUNT(DISTINCT source)::smallint AS n_sources,
            (array_agg(okrug) FILTER (WHERE okrug IS NOT NULL))[1] AS okrug,
            (array_agg(raion) FILTER (WHERE raion IS NOT NULL))[1] AS raion,
            (array_agg(building_type) FILTER (WHERE building_type IS NOT NULL))[1] AS building_type,
            (array_agg(building_era) FILTER (WHERE building_era IS NOT NULL))[1] AS building_era,
            (array_agg(is_new_building) FILTER (WHERE is_new_building IS NOT NULL))[1] AS is_new_building,
            (array_agg(total_floors) FILTER (WHERE total_floors IS NOT NULL))[1] AS total_floors,
            (array_agg(year_built) FILTER (WHERE year_built IS NOT NULL))[1] AS year_built,
            (array_agg(nearest_metro) FILTER (WHERE nearest_metro IS NOT NULL))[1] AS nearest_metro
        FROM silver_listings
        WHERE is_primary AND lat IS NOT NULL AND publication_date IS NOT NULL
        GROUP BY building_id, lat4, lon4, pub_month
        HAVING COUNT(*) >= 3
    """)
    cur.connection.commit()

    cur.execute("SELECT count(*) FROM gold_building_monthly")
    n = cur.fetchone()[0]
    log.info(f"building_monthly: {n} rows")
