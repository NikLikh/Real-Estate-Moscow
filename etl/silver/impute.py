import logging

log = logging.getLogger("re")

# маппинг source -> год публикации для kaggle-источников без дат
SOURCE_YEAR_MAP = {
    "kaggle_mrdaniilak_2021": 2021,
    "kaggle_mrdaniilak": 2019,
    "kaggle_egorkainov": 2023,
    "kaggle_hishamhaydar": 2018,
    "kaggle_ivan314sh": 2024,
    "kaggle_angultiaev": 2024,
}


def impute(cur):
    """заполняем пропуски: rooms, living/kitchen_area, publication_date"""

    # rooms из total_area по эвристике
    cur.execute("""
        UPDATE silver_staging SET rooms =
            CASE
                WHEN total_area < 35 THEN 0
                WHEN total_area < 50 THEN 1
                WHEN total_area < 75 THEN 2
                WHEN total_area < 100 THEN 3
                ELSE 4
            END
        WHERE rooms IS NULL
    """)
    log.info(f"impute rooms: {cur.rowcount}")

    # living_area по медианным пропорциям (building_type, rooms)
    cur.execute("""
        UPDATE silver_staging t SET living_area = t.total_area * sub.med_lr
        FROM (
            SELECT building_type, rooms,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (
                       ORDER BY living_area / NULLIF(total_area, 0)
                   ) AS med_lr
            FROM silver_staging
            WHERE living_area IS NOT NULL AND total_area > 0
            GROUP BY building_type, rooms
        ) sub
        WHERE t.living_area IS NULL
          AND t.building_type IS NOT DISTINCT FROM sub.building_type
          AND t.rooms = sub.rooms
    """)
    log.info(f"impute living_area: {cur.rowcount}")

    # kitchen_area аналогично
    cur.execute("""
        UPDATE silver_staging t SET kitchen_area = t.total_area * sub.med_kr
        FROM (
            SELECT building_type, rooms,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (
                       ORDER BY kitchen_area / NULLIF(total_area, 0)
                   ) AS med_kr
            FROM silver_staging
            WHERE kitchen_area IS NOT NULL AND total_area > 0
            GROUP BY building_type, rooms
        ) sub
        WHERE t.kitchen_area IS NULL
          AND t.building_type IS NOT DISTINCT FROM sub.building_type
          AND t.rooms = sub.rooms
    """)
    log.info(f"impute kitchen_area: {cur.rowcount}")

    # date_source для отслеживания откуда дата
    cur.execute("""
        ALTER TABLE silver_staging ADD COLUMN IF NOT EXISTS date_source text
    """)
    cur.execute("""
        UPDATE silver_staging SET date_source = 'original'
        WHERE pub_date IS NOT NULL AND date_source IS NULL
    """)

    # приблизительные даты для kaggle-источников без publication_date
    year_case = " ".join(
        f"WHEN '{src}' THEN '{yr}-01-01'::date + (random() * 364)::int"
        for src, yr in SOURCE_YEAR_MAP.items()
    )
    cur.execute(f"""
        UPDATE silver_staging SET
            pub_date = CASE source {year_case} END,
            date_source = 'approximated'
        WHERE pub_date IS NULL
          AND source IN ({','.join(f"'{s}'" for s in SOURCE_YEAR_MAP)})
    """)
    log.info(f"impute pub_date: {cur.rowcount}")

    cur.connection.commit()
