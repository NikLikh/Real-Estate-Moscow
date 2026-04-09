"""отдельный скрипт для геообогащения: geopandas + scipy.
запускается в subprocess чтобы не конфликтовать с Spark JVM"""
import json
import sys
from math import cos, radians
from pathlib import Path

import geopandas as gpd
import numpy as np
import psycopg2
from scipy.spatial import cKDTree

# нужен доступ к config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import DB_CONFIG, PROJECT_ROOT

DATA_DIR = PROJECT_ROOT / "data"
KREMLIN_LAT, KREMLIN_LON = 55.7520, 37.6175


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # читаем координаты
    cur.execute("SELECT lat4::float, lon4::float FROM _geo_coords_input")
    rows = cur.fetchall()
    if not rows:
        print("no coordinates to enrich")
        return

    import pandas as pd
    coords = pd.DataFrame(rows, columns=["lat4", "lon4"])
    print(f"coords: {len(coords)}")

    lat_arr = coords["lat4"].values
    lon_arr = coords["lon4"].values
    points = gpd.points_from_xy(lon_arr, lat_arr)
    gdf = gpd.GeoDataFrame(coords, geometry=points, crs="EPSG:4326")

    # округа: сначала within, потом nearest для непопавших
    gdf["okrug"] = None
    okrugs_path = DATA_DIR / "moscow_okrugs.geojson"
    if okrugs_path.exists():
        okrugs = gpd.read_file(okrugs_path).to_crs("EPSG:4326")
        if "name" in okrugs.columns:
            joined = gpd.sjoin(gdf, okrugs[["name", "geometry"]], how="left", predicate="within")
            joined = joined[~joined.index.duplicated(keep="first")]
            gdf["okrug"] = joined["name"].values

            # nearest fallback для точек без округа (на границах полигонов)
            miss = gdf["okrug"].isna()
            if miss.any():
                nearest = gpd.sjoin_nearest(
                    gdf.loc[miss, ["geometry"]], okrugs[["name", "geometry"]],
                    how="left", max_distance=0.05,  # ~5 км
                )
                nearest = nearest[~nearest.index.duplicated(keep="first")]
                gdf.loc[miss, "okrug"] = nearest["name"].values
                print(f"okrug nearest fallback: {miss.sum() - gdf['okrug'].isna().sum()}")

    # районы: аналогично within + nearest
    gdf["raion"] = None
    raions_path = DATA_DIR / "moscow_raions.geojson"
    if raions_path.exists():
        raions = gpd.read_file(raions_path).to_crs("EPSG:4326")
        if "name" in raions.columns:
            raions = raions.rename(columns={"name": "raion_name"})
            joined = gpd.sjoin(gdf[["lat4", "lon4", "geometry"]], raions[["raion_name", "geometry"]], how="left", predicate="within")
            joined = joined[~joined.index.duplicated(keep="first")]
            gdf["raion"] = joined["raion_name"].values

            miss = gdf["raion"].isna()
            if miss.any():
                nearest = gpd.sjoin_nearest(
                    gdf.loc[miss, ["lat4", "lon4", "geometry"]], raions[["raion_name", "geometry"]],
                    how="left", max_distance=0.05,
                )
                nearest = nearest[~nearest.index.duplicated(keep="first")]
                gdf.loc[miss, "raion"] = nearest["raion_name"].values
                print(f"raion nearest fallback: {miss.sum() - gdf['raion'].isna().sum()}")

    # метро
    gdf["nearest_metro"] = None
    gdf["metro_distance_m"] = np.nan
    gdf["metro_walk_min"] = np.nan
    metro_path = DATA_DIR / "moscow_metro_stations.json"
    if metro_path.exists():
        with open(metro_path, encoding="utf-8") as f:
            stations = json.load(f)
        cos_lat = cos(radians(KREMLIN_LAT))
        station_coords = np.array([[s["lat"] * 111_320, s["lon"] * 111_320 * cos_lat] for s in stations])
        station_names = [s["name"] for s in stations]
        tree = cKDTree(station_coords)
        point_coords = np.column_stack([lat_arr * 111_320, lon_arr * 111_320 * cos_lat])
        distances, indices = tree.query(point_coords, k=1)
        gdf["nearest_metro"] = [station_names[i] for i in indices]
        gdf["metro_distance_m"] = distances.round(0)
        gdf["metro_walk_min"] = (distances / 80).round(1)

    # haversine до центра
    lat1 = np.radians(lat_arr)
    lon1 = np.radians(lon_arr)
    dlat = np.radians(KREMLIN_LAT) - lat1
    dlon = np.radians(KREMLIN_LON) - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(np.radians(KREMLIN_LAT)) * np.sin(dlon / 2) ** 2
    gdf["dist_to_center_km"] = (6371 * 2 * np.arcsin(np.sqrt(a))).round(2)

    # записываем в dim_geo_coords
    cur.execute("TRUNCATE dim_geo_coords")
    sql = """INSERT INTO dim_geo_coords
        (lat4, lon4, okrug, raion, nearest_metro, metro_distance_m, metro_walk_min, dist_to_center_km)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (lat4, lon4) DO NOTHING"""

    def _v(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        return None if str(v) == "nan" else v

    batch = [
        (float(r.lat4), float(r.lon4), _v(r.okrug), _v(r.raion), _v(r.nearest_metro),
         _v(r.metro_distance_m), _v(r.metro_walk_min), _v(r.dist_to_center_km))
        for r in gdf.itertuples()
    ]
    cur.executemany(sql, batch)
    conn.commit()
    cur.close()
    conn.close()
    print(f"dim_geo_coords: {len(batch)} rows written")


if __name__ == "__main__":
    main()
