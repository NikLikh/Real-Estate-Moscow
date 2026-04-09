import logging

log = logging.getLogger("re")


def compute_features(cur):
    """вычисляемые поля: ratios, pub_year/month/quarter, quality score"""

    # ratios и pub_*
    for col, typ in [
        ("floor_ratio", "real"), ("living_ratio", "real"), ("kitchen_ratio", "real"),
        ("pub_year", "smallint"), ("pub_month", "text"), ("pub_quarter", "text"),
        ("stage", "smallint"),
        ("has_coords", "boolean"), ("has_year_built", "boolean"),
        ("has_pub_date", "boolean"), ("data_quality_score", "smallint"),
    ]:
        cur.execute(f"ALTER TABLE silver_staging ADD COLUMN IF NOT EXISTS {col} {typ}")

    cur.execute("""
        UPDATE silver_staging SET
            floor_ratio = floor::real / NULLIF(total_floors, 0),
            living_ratio = living_area / NULLIF(total_area, 0),
            kitchen_ratio = kitchen_area / NULLIF(total_area, 0),
            pub_year = EXTRACT(YEAR FROM pub_date)::smallint,
            pub_month = to_char(pub_date, 'YYYY-MM'),
            pub_quarter = EXTRACT(YEAR FROM pub_date)::text || '-Q' || EXTRACT(QUARTER FROM pub_date)::text,
            stage = completion_year - EXTRACT(YEAR FROM pub_date)::smallint
    """)
    log.info(f"features: ratios + pub_*: {cur.rowcount}")

    # building_era пересчитываем после crossfill (теперь year_built заполнен лучше)
    cur.execute("""
        UPDATE silver_staging SET building_era =
            CASE
                WHEN year_built < 1941 THEN 'Довоенный'
                WHEN year_built < 1957 THEN 'Сталинка'
                WHEN year_built < 1972 THEN 'Хрущёвка'
                WHEN year_built < 1986 THEN 'Брежневка'
                WHEN year_built < 2000 THEN 'Современный'
                ELSE 'Новый'
            END
        WHERE year_built IS NOT NULL
    """)

    # quality score -- COALESCE нужен для строковых сравнений,
    # иначе NULL = 'original' дает NULL и ломает всю сумму
    cur.execute("""
        UPDATE silver_staging SET
            has_coords = (lat IS NOT NULL),
            has_year_built = COALESCE(year_built_source = 'original', false),
            has_pub_date = COALESCE(date_source = 'original', false),
            data_quality_score = (
                (lat IS NOT NULL)::int +
                COALESCE((year_built_source = 'original')::int, 0) +
                COALESCE((date_source = 'original')::int, 0) +
                (rooms IS NOT NULL)::int +
                (living_area IS NOT NULL)::int +
                (kitchen_area IS NOT NULL)::int +
                (building_type IS NOT NULL)::int +
                (renovation IS NOT NULL)::int +
                (okrug IS NOT NULL)::int +
                (nearest_metro IS NOT NULL)::int
            )::smallint
    """)
    log.info(f"features: quality score: {cur.rowcount}")

    cur.connection.commit()
