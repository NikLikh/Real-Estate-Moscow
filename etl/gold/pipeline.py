import logging

import psycopg2

from config.settings import DB_CONFIG

log = logging.getLogger("re")


def run_gold_etl():
    """Gold ETL: silver_listings -> 6 витрин.
    building_monthly и segment_index строятся первыми,
    остальные зависят от них"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    from etl.gold.building_monthly import build_building_monthly
    from etl.gold.building_returns import build_building_returns
    from etl.gold.segment_index import build_segment_index
    from etl.gold.stage_returns import build_stage_returns
    from etl.gold.ml_features import build_ml_features
    from etl.gold.district_stats import build_district_stats

    log.info("gold: building_monthly")
    build_building_monthly(cur)

    log.info("gold: segment_index")
    build_segment_index(cur)

    log.info("gold: building_returns")
    build_building_returns(cur)

    log.info("gold: stage_returns")
    build_stage_returns(cur)

    log.info("gold: district_stats")
    build_district_stats(cur)

    log.info("gold: ml_features")
    build_ml_features(cur)

    cur.close()
    conn.close()
    log.info("gold: done")
