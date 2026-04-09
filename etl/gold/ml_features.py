import logging

log = logging.getLogger("re")


def build_ml_features(cur):
    """готовая feature-таблица для моделей.
    таргет: relative_return = return_building - return_segment (альфа)"""
    cur.execute("TRUNCATE gold_ml_features")
    cur.execute("""
        INSERT INTO gold_ml_features (
            building_id, start_month,
            relative_return_12m, relative_return_24m, above_median,
            rooms_mode, total_area_med, total_floors,
            building_type, building_era, year_built,
            renovation_mode, is_apartments_pct,
            okrug, raion, dist_to_center_km, metro_walk_min, nearest_metro,
            is_new_building, stage, developer,
            segment_ppm2, segment_ppm2_chg_3m, segment_ppm2_chg_6m, segment_ppm2_chg_12m,
            building_ppm2, ppm2_vs_segment,
            cbr_rate, mortgage_rate, mortgage_volume, usd_rub, cpi_yoy, cbr_rate_delta_3m,
            pub_year, pub_quarter, pub_month_num,
            n_obs_building, data_quality_score
        )

        -- контекст здания: агрегаты из silver для каждого building_id
        WITH building_ctx AS (
            SELECT
                hashtext(ROUND(lat::numeric, 4)::text || ',' || ROUND(lon::numeric, 4)::text)::bigint AS building_id,
                MODE() WITHIN GROUP (ORDER BY rooms) AS rooms_mode,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_area)::real AS total_area_med,
                MODE() WITHIN GROUP (ORDER BY renovation) FILTER (WHERE renovation IS NOT NULL) AS renovation_mode,
                AVG(CASE WHEN is_apartments THEN 1.0 ELSE 0.0 END)::real AS is_apartments_pct,
                (array_agg(dist_to_center_km) FILTER (WHERE dist_to_center_km IS NOT NULL))[1] AS dist_to_center_km,
                (array_agg(metro_walk_min) FILTER (WHERE metro_walk_min IS NOT NULL))[1] AS metro_walk_min,
                MODE() WITHIN GROUP (ORDER BY developer) FILTER (WHERE developer IS NOT NULL) AS developer,
                -- stage: медиана (completion_year - pub_year) для новостроек
                PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY completion_year - pub_year
                ) FILTER (WHERE completion_year IS NOT NULL AND pub_year IS NOT NULL)::smallint AS stage,
                AVG(data_quality_score)::smallint AS data_quality_score
            FROM silver_listings
            WHERE lat IS NOT NULL
            GROUP BY building_id
        ),

        -- rooms_bucket для матчинга с segment_index
        bm_with_rooms AS (
            SELECT bm.*,
                CASE
                    WHEN ctx.rooms_mode IS NULL THEN 'unknown'
                    WHEN ctx.rooms_mode <= 1 THEN '0-1'
                    WHEN ctx.rooms_mode = 2 THEN '2'
                    WHEN ctx.rooms_mode = 3 THEN '3'
                    ELSE '4+'
                END AS rooms_bucket,
                ctx.rooms_mode, ctx.total_area_med, ctx.renovation_mode,
                ctx.is_apartments_pct, ctx.dist_to_center_km, ctx.metro_walk_min,
                ctx.developer, ctx.stage AS bld_stage, ctx.data_quality_score
            FROM gold_building_monthly bm
            LEFT JOIN building_ctx ctx ON bm.building_id = ctx.building_id
        ),

        -- segment index с lag-ами для трендов
        seg_with_lag AS (
            SELECT *,
                LAG(raw_med_ppm2, 3)  OVER w AS seg_lag_3,
                LAG(raw_med_ppm2, 6)  OVER w AS seg_lag_6,
                LAG(raw_med_ppm2, 12) OVER w AS seg_lag_12
            FROM gold_segment_index
            WINDOW w AS (PARTITION BY segment_key ORDER BY pub_month)
        ),

        base AS (
            SELECT
                bm.building_id, bm.pub_month,
                bm.med_ppm2,
                bm.okrug, bm.raion, bm.building_type, bm.building_era,
                bm.year_built, bm.is_new_building, bm.total_floors, bm.nearest_metro,
                bm.n_obs,
                bm.rooms_mode, bm.total_area_med, bm.renovation_mode,
                bm.is_apartments_pct, bm.dist_to_center_km, bm.metro_walk_min,
                bm.developer, bm.bld_stage, bm.data_quality_score,

                -- returns за 12 и 24 мес
                br12.return_pct AS building_return_12m,
                br24.return_pct AS building_return_24m,

                -- segment context: матчим по okrug + is_new_building + rooms_bucket
                seg.raw_med_ppm2 AS segment_ppm2,
                CASE WHEN seg.seg_lag_3 > 0
                     THEN (seg.raw_med_ppm2::real / seg.seg_lag_3 - 1) * 100 END AS segment_ppm2_chg_3m,
                CASE WHEN seg.seg_lag_6 > 0
                     THEN (seg.raw_med_ppm2::real / seg.seg_lag_6 - 1) * 100 END AS segment_ppm2_chg_6m,
                CASE WHEN seg.seg_lag_12 > 0
                     THEN (seg.raw_med_ppm2::real / seg.seg_lag_12 - 1) * 100 END AS segment_ppm2_chg_12m

            FROM bm_with_rooms bm

            LEFT JOIN gold_building_returns br12
              ON bm.building_id = br12.building_id
             AND bm.pub_month = br12.start_month
             AND br12.horizon_months = 12

            LEFT JOIN gold_building_returns br24
              ON bm.building_id = br24.building_id
             AND bm.pub_month = br24.start_month
             AND br24.horizon_months = 24

            LEFT JOIN seg_with_lag seg
              ON seg.segment_key = COALESCE(bm.okrug,
                    CASE WHEN bm.is_new_building THEN '_MO' ELSE '_UNK' END
                 ) || '|'
                    || COALESCE(bm.is_new_building, false)::text || '|'
                    || bm.rooms_bucket
             AND seg.pub_month = bm.pub_month
        )

        SELECT
            building_id,
            pub_month AS start_month,
            -- таргеты: альфа = building_return - segment_return
            building_return_12m - segment_ppm2_chg_12m AS relative_return_12m,
            building_return_24m - COALESCE(segment_ppm2_chg_12m, 0) * 2 AS relative_return_24m,
            CASE WHEN building_return_12m IS NOT NULL
                 THEN building_return_12m > COALESCE(segment_ppm2_chg_12m, 0)
            END AS above_median,
            rooms_mode, total_area_med, total_floors,
            building_type, building_era, year_built,
            renovation_mode, is_apartments_pct,
            okrug, raion, dist_to_center_km, metro_walk_min, nearest_metro,
            is_new_building, bld_stage, developer,
            segment_ppm2,
            segment_ppm2_chg_3m, segment_ppm2_chg_6m, segment_ppm2_chg_12m,
            med_ppm2 AS building_ppm2,
            CASE WHEN segment_ppm2 > 0 THEN med_ppm2::real / segment_ppm2 END AS ppm2_vs_segment,
            -- макро-фичи пока NULL, заполнятся из dim_macro_monthly
            NULL::real, NULL::real, NULL::real, NULL::real, NULL::real, NULL::real,
            SUBSTRING(pub_month, 1, 4)::smallint AS pub_year,
            CEIL(SUBSTRING(pub_month, 6, 2)::int / 3.0)::smallint AS pub_quarter,
            SUBSTRING(pub_month, 6, 2)::smallint AS pub_month_num,
            n_obs AS n_obs_building,
            data_quality_score
        FROM base
        WHERE building_return_12m IS NOT NULL OR building_return_24m IS NOT NULL
    """)
    cur.connection.commit()

    cur.execute("SELECT count(*) FROM gold_ml_features")
    log.info(f"ml_features: {cur.fetchone()[0]} rows")
