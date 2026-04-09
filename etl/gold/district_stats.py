import logging

log = logging.getLogger("re")


def build_district_stats(cur):
    """агрегаты по районам для дашбордов: цены, объемы, структура, динамика"""
    cur.execute("TRUNCATE gold_district_stats")
    cur.execute("""
        INSERT INTO gold_district_stats (
            okrug, raion, is_new_building, pub_month,
            n_listings, n_buildings, med_ppm2, avg_ppm2,
            ppm2_q25, ppm2_q75, med_price,
            avg_area, avg_rooms,
            pct_studio, pct_1room, pct_2room, pct_3plus,
            ppm2_chg_1m, ppm2_chg_3m, ppm2_chg_12m
        )
        WITH base AS (
            SELECT
                okrug, COALESCE(raion, '') AS raion, COALESCE(is_new_building, false) AS is_new_building, pub_month,
                price, price_per_m2, total_area, rooms,
                hashtext(ROUND(lat::numeric, 4)::text || ',' || ROUND(lon::numeric, 4)::text)::bigint AS building_id
            FROM silver_listings
            WHERE is_primary AND okrug IS NOT NULL AND pub_month IS NOT NULL
        ),
        agg AS (
            SELECT
                okrug, raion, is_new_building, pub_month,
                COUNT(*) AS n_listings,
                COUNT(DISTINCT building_id) AS n_buildings,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2)::bigint AS med_ppm2,
                AVG(price_per_m2)::bigint AS avg_ppm2,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_per_m2)::bigint AS ppm2_q25,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_per_m2)::bigint AS ppm2_q75,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::bigint AS med_price,
                AVG(total_area)::real AS avg_area,
                AVG(rooms)::real AS avg_rooms,
                (SUM(CASE WHEN rooms = 0 THEN 1 ELSE 0 END)::real / COUNT(*)) AS pct_studio,
                (SUM(CASE WHEN rooms = 1 THEN 1 ELSE 0 END)::real / COUNT(*)) AS pct_1room,
                (SUM(CASE WHEN rooms = 2 THEN 1 ELSE 0 END)::real / COUNT(*)) AS pct_2room,
                (SUM(CASE WHEN rooms >= 3 THEN 1 ELSE 0 END)::real / COUNT(*)) AS pct_3plus
            FROM base
            GROUP BY okrug, raion, is_new_building, pub_month
        ),
        with_lag AS (
            SELECT *,
                LAG(med_ppm2, 1)  OVER w AS lag1,
                LAG(med_ppm2, 3)  OVER w AS lag3,
                LAG(med_ppm2, 12) OVER w AS lag12
            FROM agg
            WINDOW w AS (PARTITION BY okrug, raion, is_new_building ORDER BY pub_month)
        )
        SELECT
            okrug, raion, is_new_building, pub_month,
            n_listings, n_buildings, med_ppm2, avg_ppm2,
            ppm2_q25, ppm2_q75, med_price,
            avg_area, avg_rooms,
            pct_studio, pct_1room, pct_2room, pct_3plus,
            CASE WHEN lag1 > 0 THEN (med_ppm2::real / lag1 - 1) * 100 END AS ppm2_chg_1m,
            CASE WHEN lag3 > 0 THEN (med_ppm2::real / lag3 - 1) * 100 END AS ppm2_chg_3m,
            CASE WHEN lag12 > 0 THEN (med_ppm2::real / lag12 - 1) * 100 END AS ppm2_chg_12m
        FROM with_lag
    """)
    cur.connection.commit()

    cur.execute("SELECT count(*) FROM gold_district_stats")
    log.info(f"district_stats: {cur.fetchone()[0]} rows")
