"""геообогащение: округа, районы, метро, расстояние до центра.
geopandas и scipy запускаются в отдельном процессе чтобы избежать
DLL-конфликтов на Windows"""
import logging
import subprocess
import sys
from pathlib import Path

from config.settings import PROJECT_ROOT

log = logging.getLogger("re")


def _run_geo_build():
    """запускаем _build_and_save_geo.py в отдельном процессе"""
    script = Path(__file__).parent / "_build_and_save_geo.py"
    result = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True, text=True, timeout=600,
        cwd=str(PROJECT_ROOT),
    )
    if result.returncode != 0:
        log.error(f"geo build failed:\n{result.stderr}")
        raise RuntimeError(f"geo build failed: {result.stderr[-500:]}")
    log.info(result.stdout.strip().split("\n")[-1])


def geo_enrich(cur):
    """обогащаем silver_staging округами, районами, метро и расстоянием до центра"""

    # добавляем geo-колонки если их нет
    for col, typ in [
        ("okrug", "text"), ("raion", "text"),
        ("nearest_metro", "text"), ("metro_distance_m", "real"),
        ("metro_walk_min", "real"), ("dist_to_center_km", "real"),
    ]:
        cur.execute(f"ALTER TABLE silver_staging ADD COLUMN IF NOT EXISTS {col} {typ}")

    # dim_geo_coords должна существовать (create_silver_gold.sql)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_geo_coords (
            lat4 NUMERIC(8,4) NOT NULL, lon4 NUMERIC(8,4) NOT NULL,
            okrug TEXT, raion TEXT, nearest_metro TEXT,
            metro_distance_m REAL, metro_walk_min REAL, dist_to_center_km REAL,
            PRIMARY KEY (lat4, lon4)
        )
    """)

    # уникальные координаты -> таблица для geo-скрипта
    cur.execute("DROP TABLE IF EXISTS _geo_coords_input")
    cur.execute("""
        CREATE TABLE _geo_coords_input AS
        SELECT DISTINCT
            ROUND(lat::numeric, 4) AS lat4,
            ROUND(lon::numeric, 4) AS lon4
        FROM silver_staging
        WHERE lat IS NOT NULL
    """)
    cur.connection.commit()

    cur.execute("SELECT count(*) FROM _geo_coords_input")
    n = cur.fetchone()[0]
    log.info(f"geo_enrich: {n} unique coords")

    if n == 0:
        return

    # запускаем геообогащение в subprocess
    _run_geo_build()

    # join результатов в silver_staging
    cur.execute("""
        UPDATE silver_staging t SET
            okrug = g.okrug,
            raion = g.raion,
            nearest_metro = g.nearest_metro,
            metro_distance_m = g.metro_distance_m,
            metro_walk_min = g.metro_walk_min,
            dist_to_center_km = g.dist_to_center_km
        FROM dim_geo_coords g
        WHERE t.lat IS NOT NULL
          AND ROUND(t.lat::numeric, 4) = g.lat4
          AND ROUND(t.lon::numeric, 4) = g.lon4
    """)
    cur.connection.commit()
    log.info(f"geo_enrich: updated {cur.rowcount} rows")

    cur.execute("DROP TABLE IF EXISTS _geo_coords_input")
    cur.connection.commit()
