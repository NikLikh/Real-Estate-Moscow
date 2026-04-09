import logging

log = logging.getLogger("re")


def _count(cur, query):
    cur.execute(query)
    return cur.fetchone()[0]


def check_silver(cur):
    """проверяем silver_listings после ETL"""
    total = _count(cur, "SELECT count(*) FROM silver_listings")
    primary = _count(cur, "SELECT count(*) FROM silver_listings WHERE is_primary")
    no_price = _count(cur, "SELECT count(*) FROM silver_listings WHERE price_per_m2 IS NULL")
    no_year = _count(cur, "SELECT count(*) FROM silver_listings WHERE year_built IS NULL AND is_primary")
    has_coords = _count(cur, "SELECT count(*) FROM silver_listings WHERE has_coords AND is_primary")
    has_okrug = _count(cur, "SELECT count(*) FROM silver_listings WHERE okrug IS NOT NULL AND is_primary")
    dup_groups = _count(cur, "SELECT count(DISTINCT dedup_group_id) FROM silver_listings WHERE group_size > 1")

    year_null_pct = no_year / primary * 100 if primary else 0
    coords_pct = has_coords / primary * 100 if primary else 0

    log.info("=== Silver quality ===")
    log.info(f"total: {total}, primary: {primary}")
    log.info(f"price_per_m2 NULL: {no_price}")
    log.info(f"year_built NULL: {no_year} ({year_null_pct:.1f}% primary)")
    log.info(f"has_coords: {has_coords} ({coords_pct:.1f}% primary)")
    log.info(f"okrug: {has_okrug}")
    log.info(f"dedup groups: {dup_groups}")

    ok = True
    if total < 100_000:
        log.warning(f"total {total} < 100K")
        ok = False
    if no_price > 100:
        log.warning(f"price_per_m2 NULL: {no_price} > 100")
        ok = False
    if year_null_pct > 10:
        log.warning(f"year_built NULL {year_null_pct:.1f}% > 10%")
        ok = False

    # проверки нормализации

    bad_region = _count(cur, """
        SELECT count(*) FROM silver_listings
        WHERE is_primary AND region IS NOT NULL
          AND region NOT IN ('Москва', 'Московская область')
    """)
    if bad_region > 0:
        log.warning(f"region not normalized: {bad_region} rows")
        ok = False

    neg_rooms = _count(cur, """
        SELECT count(*) FROM silver_listings WHERE rooms < 0
    """)
    if neg_rooms > 0:
        log.warning(f"negative rooms (un-normalized studios): {neg_rooms}")
        ok = False

    bad_floor = _count(cur, """
        SELECT count(*) FROM silver_listings
        WHERE is_primary AND floor IS NOT NULL AND total_floors IS NOT NULL
          AND floor > total_floors
    """)
    if bad_floor > 0:
        log.warning(f"floor > total_floors: {bad_floor}")
        ok = False

    bad_area = _count(cur, """
        SELECT count(*) FROM silver_listings
        WHERE is_primary AND living_area IS NOT NULL
          AND living_area > total_area
    """)
    if bad_area > 0:
        log.warning(f"living_area > total_area: {bad_area}")
        ok = False

    null_nb = _count(cur, """
        SELECT count(*) FROM silver_listings WHERE is_new_building IS NULL
    """)
    if null_nb > 0:
        log.warning(f"is_new_building NULL: {null_nb}")
        ok = False

    extreme_ppm2 = _count(cur, """
        SELECT count(*) FROM silver_listings
        WHERE is_primary AND price_per_m2 IS NOT NULL
          AND (price_per_m2 < 20000 OR price_per_m2 > 5000000)
    """)
    if extreme_ppm2 > 0:
        log.warning(f"extreme price_per_m2: {extreme_ppm2}")
        ok = False

    return ok


def check_gold(cur):
    """проверяем Gold-витрины"""
    tables = {
        "gold_building_monthly": 1000,
        "gold_building_returns": 100,
        "gold_segment_index": 10,
        "gold_district_stats": 10,
    }

    log.info("=== Gold quality ===")
    ok = True
    for table, min_rows in tables.items():
        n = _count(cur, f"SELECT count(*) FROM {table}")
        status = "ok" if n >= min_rows else "WARN"
        log.info(f"{table}: {n} rows [{status}]")
        if n < min_rows:
            ok = False

    return ok
