import logging

log = logging.getLogger("re")


def build_segment_index(cur):
    """ценовой индекс по сегментам (okrug, is_new_building, rooms_bucket)"""
    cur.execute("TRUNCATE gold_segment_index")
    cur.execute("""
        INSERT INTO gold_segment_index (
            segment_key, okrug, is_new_building, rooms_bucket, pub_month,
            raw_med_ppm2, n_obs, ppm2_q25, ppm2_q75, ppm2_std,
            hedonic_index, hedonic_ppm2
        )
        SELECT
            segment_key,
            (array_agg(okrug) FILTER (WHERE okrug IS NOT NULL))[1] AS okrug,
            COALESCE(bool_or(is_new_building), false) AS is_new_building,
            rooms_bucket,
            pub_month,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_per_m2)::bigint AS raw_med_ppm2,
            COUNT(*) AS n_obs,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_per_m2)::bigint AS ppm2_q25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_per_m2)::bigint AS ppm2_q75,
            STDDEV(price_per_m2)::real AS ppm2_std,
            NULL::real AS hedonic_index,
            NULL::bigint AS hedonic_ppm2
        FROM (
            SELECT *,
                COALESCE(okrug,
                    CASE WHEN region = 'Московская область' THEN '_MO' ELSE '_UNK' END
                ) || '|' || COALESCE(is_new_building, false)::text || '|' ||
                CASE
                    WHEN rooms IS NULL THEN 'unknown'
                    WHEN rooms <= 1 THEN '0-1'
                    WHEN rooms = 2 THEN '2'
                    WHEN rooms = 3 THEN '3'
                    ELSE '4+'
                END AS segment_key,
                CASE
                    WHEN rooms IS NULL THEN 'unknown'
                    WHEN rooms <= 1 THEN '0-1'
                    WHEN rooms = 2 THEN '2'
                    WHEN rooms = 3 THEN '3'
                    ELSE '4+'
                END AS rooms_bucket
            FROM silver_listings
            WHERE is_primary AND publication_date IS NOT NULL
        ) t
        GROUP BY segment_key, rooms_bucket, pub_month
    """)
    cur.connection.commit()

    cur.execute("SELECT count(*) FROM gold_segment_index")
    n = cur.fetchone()[0]
    log.info(f"segment_index: {n} rows")
