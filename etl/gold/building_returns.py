import logging

log = logging.getLogger("re")

HORIZONS = [6, 12, 18, 24, 36]


def build_building_returns(cur):
    """доходности по зданиям на горизонтах 6/12/18/24/36 месяцев"""
    cur.execute("TRUNCATE gold_building_returns")

    for h in HORIZONS:
        cur.execute(f"""
            INSERT INTO gold_building_returns (
                building_id, lat4, lon4, start_month, horizon_months,
                start_ppm2, end_ppm2, return_pct, annualized_return,
                okrug, raion, is_new_building
            )
            SELECT
                s.building_id, s.lat4, s.lon4,
                s.pub_month AS start_month,
                {h}::smallint AS horizon_months,
                s.med_ppm2 AS start_ppm2,
                e.med_ppm2 AS end_ppm2,
                (e.med_ppm2::real / s.med_ppm2 - 1) * 100 AS return_pct,
                (POWER(e.med_ppm2::real / s.med_ppm2, 12.0 / {h}) - 1) * 100 AS annualized_return,
                s.okrug, s.raion, s.is_new_building
            FROM gold_building_monthly s
            JOIN gold_building_monthly e
              ON s.building_id = e.building_id
             AND e.pub_month = to_char(
                     (s.pub_month || '-01')::date + interval '{h} months',
                     'YYYY-MM'
                 )
            WHERE s.med_ppm2 > 0
        """)
        log.info(f"building_returns h={h}: {cur.rowcount}")

    cur.connection.commit()
    cur.execute("SELECT count(*) FROM gold_building_returns")
    log.info(f"building_returns total: {cur.fetchone()[0]} rows")
