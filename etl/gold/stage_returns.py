import logging

log = logging.getLogger("re")


def build_stage_returns(cur):
    """доходность по стадии стройки (лет до сдачи)"""
    cur.execute("TRUNCATE gold_stage_returns")

    # общие агрегаты по (stage, horizon) + разбивка по году входа
    cur.execute("""
        INSERT INTO gold_stage_returns (
            stage, horizon_months, entry_year,
            med_return_pct, avg_return_pct, q25_return_pct, q75_return_pct,
            pct_positive, sharpe_like, n_obs
        )
        WITH nb AS (
            SELECT DISTINCT
                hashtext(ROUND(lat::numeric, 4)::text || ',' || ROUND(lon::numeric, 4)::text)::bigint AS building_id,
                completion_year
            FROM silver_listings
            WHERE is_primary AND is_new_building AND completion_year IS NOT NULL
        ),
        br_nb AS (
            SELECT
                br.*,
                nb.completion_year,
                nb.completion_year - SUBSTRING(br.start_month, 1, 4)::smallint AS stage,
                SUBSTRING(br.start_month, 1, 4)::smallint AS start_year
            FROM gold_building_returns br
            JOIN nb ON br.building_id = nb.building_id
        ),
        -- агрегаты без разбивки по году
        agg_all AS (
            SELECT
                stage, horizon_months, 0::smallint AS entry_year,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_pct)::real AS med_return_pct,
                AVG(return_pct)::real AS avg_return_pct,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY return_pct)::real AS q25_return_pct,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY return_pct)::real AS q75_return_pct,
                (SUM(CASE WHEN return_pct > 0 THEN 1 ELSE 0 END)::real / COUNT(*)) AS pct_positive,
                CASE WHEN STDDEV(return_pct) > 0
                     THEN (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_pct) / STDDEV(return_pct))::real
                END AS sharpe_like,
                COUNT(*) AS n_obs
            FROM br_nb
            GROUP BY stage, horizon_months
        ),
        -- с разбивкой по году входа
        agg_by_year AS (
            SELECT
                stage, horizon_months, start_year AS entry_year,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_pct)::real AS med_return_pct,
                AVG(return_pct)::real AS avg_return_pct,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY return_pct)::real AS q25_return_pct,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY return_pct)::real AS q75_return_pct,
                (SUM(CASE WHEN return_pct > 0 THEN 1 ELSE 0 END)::real / COUNT(*)) AS pct_positive,
                CASE WHEN STDDEV(return_pct) > 0
                     THEN (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY return_pct) / STDDEV(return_pct))::real
                END AS sharpe_like,
                COUNT(*) AS n_obs
            FROM br_nb
            GROUP BY stage, horizon_months, start_year
        )
        SELECT * FROM agg_all
        UNION ALL
        SELECT * FROM agg_by_year
    """)
    cur.connection.commit()

    cur.execute("SELECT count(*) FROM gold_stage_returns")
    log.info(f"stage_returns: {cur.fetchone()[0]} rows")
