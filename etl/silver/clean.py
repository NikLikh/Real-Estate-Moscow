import logging

log = logging.getLogger("re")


def clean(cur):
    """фильтрация мусора, парсинг дат, вычисление price_per_m2"""

    # lat/lon = 0 считаем как отсутствие координат
    cur.execute("""
        UPDATE silver_staging SET lat = NULL, lon = NULL
        WHERE lat = 0 OR lon = 0
    """)
    log.info(f"clean: zeroed coords -> NULL: {cur.rowcount}")

    # убираем выбросы по цене, площади, координатам вне bbox
    cur.execute("""
        DELETE FROM silver_staging
        WHERE price <= 0
           OR price >= 1000000000
           OR total_area NOT BETWEEN 5 AND 500
           OR (lat IS NOT NULL AND NOT (lat BETWEEN 54.2 AND 57.0
                                        AND lon BETWEEN 35.0 AND 40.5))
    """)
    cur.connection.commit()
    log.info(f"clean: removed {cur.rowcount} invalid rows")

    # площади: living и kitchen не могут быть больше total
    cur.execute("""
        UPDATE silver_staging SET living_area = NULL
        WHERE living_area IS NOT NULL AND living_area > total_area
    """)
    log.info(f"clean: living_area > total_area, обнулено: {cur.rowcount}")

    cur.execute("""
        UPDATE silver_staging SET kitchen_area = NULL
        WHERE kitchen_area IS NOT NULL AND kitchen_area > total_area
    """)
    log.info(f"clean: kitchen_area > total_area, обнулено: {cur.rowcount}")

    cur.execute("""
        UPDATE silver_staging SET living_area = NULL, kitchen_area = NULL
        WHERE living_area IS NOT NULL AND kitchen_area IS NOT NULL
          AND living_area + kitchen_area > total_area * 1.05
    """)
    log.info(f"clean: living+kitchen > total*1.05, обнулены обе: {cur.rowcount}")

    # этажи: floor <= 0, total_floors <= 0, floor > total_floors
    cur.execute("""
        UPDATE silver_staging SET floor = NULL
        WHERE floor IS NOT NULL AND floor <= 0
    """)
    cur.execute("""
        UPDATE silver_staging SET total_floors = NULL
        WHERE total_floors IS NOT NULL AND total_floors <= 0
    """)
    cur.execute("""
        UPDATE silver_staging SET floor = NULL, total_floors = NULL
        WHERE floor IS NOT NULL AND total_floors IS NOT NULL
          AND floor > total_floors
    """)
    log.info(f"clean: невалидные floor/total_floors обнулены: {cur.rowcount}")

    # year_built вне диапазона 1800..now+5
    cur.execute("""
        UPDATE silver_staging SET year_built = NULL
        WHERE year_built IS NOT NULL
          AND (year_built < 1800
               OR year_built > EXTRACT(YEAR FROM CURRENT_DATE)::int + 5)
    """)
    log.info(f"clean: year_built вне диапазона, обнулено: {cur.rowcount}")

    # ceiling_height вне 2.0..6.0
    cur.execute("""
        UPDATE silver_staging SET ceiling_height = NULL
        WHERE ceiling_height IS NOT NULL
          AND (ceiling_height < 2.0 OR ceiling_height > 6.0)
    """)
    log.info(f"clean: ceiling_height вне диапазона, обнулено: {cur.rowcount}")

    # парсим publication_date: два формата (yyyy-MM-dd и d/M/yyyy)
    # to_date кидает ошибку на мусор, поэтому фильтруем regex-ом
    cur.execute("""
        ALTER TABLE silver_staging
            ADD COLUMN IF NOT EXISTS pub_date date
    """)
    # формат yyyy-MM-dd (с опциональной частью Thh:mm:ss)
    cur.execute(r"""
        UPDATE silver_staging
        SET pub_date = LEFT(publication_date, 10)::date
        WHERE pub_date IS NULL
          AND publication_date ~ '^\d{4}-\d{2}-\d{2}'
    """)
    log.info(f"clean: parsed pub_date yyyy-MM-dd: {cur.rowcount}")
    # формат d/M/yyyy (romanbaster)
    cur.execute(r"""
        UPDATE silver_staging
        SET pub_date = to_date(publication_date, 'DD/MM/YYYY')
        WHERE pub_date IS NULL
          AND publication_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
    """)
    log.info(f"clean: parsed pub_date d/M/yyyy: {cur.rowcount}")

    # completion_year из completion_date (извлекаем первые 4 цифры)
    cur.execute("""
        ALTER TABLE silver_staging
            ADD COLUMN IF NOT EXISTS completion_year smallint
    """)
    cur.execute(r"""
        UPDATE silver_staging
        SET completion_year = (regexp_match(completion_date, '(\d{4})'))[1]::smallint
        WHERE completion_date IS NOT NULL
          AND completion_date ~ '\d{4}'
          AND completion_year IS NULL
    """)
    log.info(f"clean: completion_year: {cur.rowcount}")

    # price_per_m2 где отсутствует
    cur.execute("""
        UPDATE silver_staging
        SET price_per_m2 = (price / NULLIF(total_area, 0))::bigint
        WHERE price_per_m2 IS NULL OR price_per_m2 = 0
    """)
    log.info(f"clean: computed price_per_m2: {cur.rowcount}")

    # выбросы price_per_m2 за пределами 20K-5M руб/м2
    cur.execute("""
        DELETE FROM silver_staging
        WHERE price_per_m2 IS NOT NULL
          AND (price_per_m2 < 20000 OR price_per_m2 > 5000000)
    """)
    log.info(f"clean: extreme price_per_m2 removed: {cur.rowcount}")

    cur.connection.commit()
