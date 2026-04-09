import logging

log = logging.getLogger("re")

TBL = "silver_staging"


def crossfill(cur):
    """заполняем year_built в PostgreSQL -- 5 шагов с убывающей точностью.
    PG справляется с GROUP BY на 3M строк без проблем с памятью"""

    # колонка для отслеживания источника заполнения
    cur.execute(f"""
        ALTER TABLE {TBL} ADD COLUMN IF NOT EXISTS year_built_source text
    """)
    cur.execute(f"""
        UPDATE {TBL} SET year_built_source = 'original'
        WHERE year_built IS NOT NULL
    """)
    cur.connection.commit()
    log.info(f"crossfill: original year_built = {cur.rowcount}")

    # шаг 1: по координатам 4 знака (~11м, один дом)
    cur.execute(f"""
        UPDATE {TBL} t SET year_built = sub.yr, year_built_source = 'coords_4'
        FROM (
            SELECT ROUND(lat::numeric, 4) as lat4, ROUND(lon::numeric, 4) as lon4,
                   MODE() WITHIN GROUP (ORDER BY year_built) as yr
            FROM {TBL}
            WHERE year_built IS NOT NULL AND lat IS NOT NULL
            GROUP BY lat4, lon4
        ) sub
        WHERE t.year_built IS NULL AND t.lat IS NOT NULL
          AND ROUND(t.lat::numeric, 4) = sub.lat4
          AND ROUND(t.lon::numeric, 4) = sub.lon4
    """)
    cur.connection.commit()
    log.info(f"crossfill coords_4: {cur.rowcount}")

    # шаг 2: по координатам 3 знака (~111м, соседние дома)
    cur.execute(f"""
        UPDATE {TBL} t SET year_built = sub.yr, year_built_source = 'coords_3'
        FROM (
            SELECT ROUND(lat::numeric, 3) as lat3, ROUND(lon::numeric, 3) as lon3,
                   MODE() WITHIN GROUP (ORDER BY year_built) as yr
            FROM {TBL}
            WHERE year_built IS NOT NULL AND lat IS NOT NULL
            GROUP BY lat3, lon3
        ) sub
        WHERE t.year_built IS NULL AND t.lat IS NOT NULL
          AND ROUND(t.lat::numeric, 3) = sub.lat3
          AND ROUND(t.lon::numeric, 3) = sub.lon3
    """)
    cur.connection.commit()
    log.info(f"crossfill coords_3: {cur.rowcount}")

    # шаг 3: по адресу (city, street, house)
    cur.execute(f"""
        UPDATE {TBL} t SET year_built = sub.yr, year_built_source = 'address'
        FROM (
            SELECT city, street, house,
                   MODE() WITHIN GROUP (ORDER BY year_built) as yr
            FROM {TBL}
            WHERE year_built IS NOT NULL AND street IS NOT NULL AND house IS NOT NULL
            GROUP BY city, street, house
        ) sub
        WHERE t.year_built IS NULL
          AND t.street IS NOT NULL AND t.house IS NOT NULL
          AND t.city IS NOT DISTINCT FROM sub.city
          AND t.street = sub.street AND t.house = sub.house
    """)
    cur.connection.commit()
    log.info(f"crossfill address: {cur.rowcount}")

    # шаг 4: медиана по (building_type, region, total_floors)
    cur.execute(f"""
        UPDATE {TBL} t SET year_built = sub.yr, year_built_source = 'category'
        FROM (
            SELECT building_type, region, total_floors,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY year_built)::smallint as yr
            FROM {TBL}
            WHERE year_built IS NOT NULL
              AND building_type IS NOT NULL AND region IS NOT NULL AND total_floors IS NOT NULL
            GROUP BY building_type, region, total_floors
            HAVING COUNT(*) > 10
        ) sub
        WHERE t.year_built IS NULL
          AND t.building_type = sub.building_type
          AND t.region = sub.region
          AND t.total_floors = sub.total_floors
    """)
    cur.connection.commit()
    log.info(f"crossfill category: {cur.rowcount}")

    # шаг 5: fallback медиана по (building_type, total_floors) без region
    cur.execute(f"""
        UPDATE {TBL} t SET year_built = sub.yr, year_built_source = 'fallback'
        FROM (
            SELECT building_type, total_floors,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY year_built)::smallint as yr
            FROM {TBL}
            WHERE year_built IS NOT NULL
              AND building_type IS NOT NULL AND total_floors IS NOT NULL
            GROUP BY building_type, total_floors
            HAVING COUNT(*) > 30
        ) sub
        WHERE t.year_built IS NULL
          AND t.building_type = sub.building_type
          AND t.total_floors = sub.total_floors
    """)
    cur.connection.commit()
    log.info(f"crossfill fallback: {cur.rowcount}")

    cur.execute(f"SELECT count(*) FROM {TBL} WHERE year_built IS NULL")
    nulls = cur.fetchone()[0]
    cur.execute(f"SELECT count(*) FROM {TBL}")
    total = cur.fetchone()[0]
    log.info(f"crossfill year_built done: {nulls}/{total} NULL ({nulls/total*100:.1f}%)")

    # ceiling_height по координатам 4 знака (медиана, min 3 наблюдения)
    cur.execute(f"""
        UPDATE {TBL} t SET ceiling_height = sub.ch
        FROM (
            SELECT ROUND(lat::numeric, 4) AS lat4, ROUND(lon::numeric, 4) AS lon4,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ceiling_height)::real AS ch
            FROM {TBL}
            WHERE ceiling_height IS NOT NULL AND lat IS NOT NULL
            GROUP BY lat4, lon4
            HAVING COUNT(*) >= 3
        ) sub
        WHERE t.ceiling_height IS NULL AND t.lat IS NOT NULL
          AND ROUND(t.lat::numeric, 4) = sub.lat4
          AND ROUND(t.lon::numeric, 4) = sub.lon4
    """)
    cur.connection.commit()
    log.info(f"crossfill ceiling_height by coords: {cur.rowcount}")
