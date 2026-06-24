import json
import os
import sys

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from app.backend.db import fetch_all, get_engine
from pipeline.ml.export import build_export

app = FastAPI(title="real_estate")

CHECKPOINTS = os.path.join(os.path.dirname(__file__), "..", "..", "checkpoints")
EXPORT_PATH = os.path.join(CHECKPOINTS, "current_listings.xlsx")
META_PATH = os.path.join(CHECKPOINTS, "hot_model_meta.json")
PPM2_MIN, PPM2_MAX = 50000, 2000000


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/model/meta")
def model_meta():
    if not os.path.exists(META_PATH):
        return {}
    with open(META_PATH) as fh:
        m = json.load(fh)
    return {"trained_at": m.get("trained_at"), "pr_auc": m.get("pr_auc"),
            "auc": m.get("auc"), "n_features": len(m.get("features", []))}


@app.get("/dashboard/price-index")
def price_index(municipality=Query(None), is_new_building=Query(None)):
    sql = (
        "select month, "
        "round(sum(median_ppm2 * n_points)::numeric / nullif(sum(n_points), 0))::bigint as median_ppm2, "
        "sum(n_points) as n_points "
        "from marts.price_index_monthly where month >= '2026-01-01'"
    )
    params = []
    if municipality:
        sql += " and municipality = %s"
        params.append(municipality)
    if is_new_building is not None:
        sql += " and is_new_building = %s"
        params.append(is_new_building in ("true", "1", True))
    sql += " group by month order by month"
    return fetch_all(sql, tuple(params))


@app.get("/dashboard/segmentation")
def segmentation():
    by_rooms = fetch_all(
        """
        select
            case when is_studio then 'Студия'
                 when rooms is null then 'Не указано'
                 when rooms >= 5 then '5+'
                 else rooms::text end as room_group,
            percentile_cont(0.05) within group (order by price_per_m2) as p05,
            percentile_cont(0.25) within group (order by price_per_m2) as q1,
            percentile_cont(0.5) within group (order by price_per_m2) as median,
            percentile_cont(0.75) within group (order by price_per_m2) as q3,
            percentile_cont(0.95) within group (order by price_per_m2) as p95,
            count(*) as n
        from marts.current_listings
        where price_per_m2 between %s and %s
        group by room_group having count(*) > 50
        """,
        (PPM2_MIN, PPM2_MAX),
    )
    new_vs_secondary = fetch_all(
        """
        select
            is_new_building,
            percentile_cont(0.05) within group (order by price_per_m2) as p05,
            percentile_cont(0.25) within group (order by price_per_m2) as q1,
            percentile_cont(0.5) within group (order by price_per_m2) as median,
            percentile_cont(0.75) within group (order by price_per_m2) as q3,
            percentile_cont(0.95) within group (order by price_per_m2) as p95,
            count(*) as n
        from marts.current_listings
        where price_per_m2 between %s and %s
        group by is_new_building order by is_new_building
        """,
        (PPM2_MIN, PPM2_MAX),
    )
    return {"by_rooms": by_rooms, "new_vs_secondary": new_vs_secondary}


@app.get("/dashboard/geo")
def geo():
    return fetch_all(
        """
        select municipality,
               percentile_cont(0.5) within group (order by price_per_m2) as median_ppm2,
               count(*) as n_active, avg(lat) as lat, avg(lon) as lon
        from marts.current_listings
        where municipality is not null and lat is not null
          and price_per_m2 between %s and %s
        group by municipality order by median_ppm2 desc
        """,
        (PPM2_MIN, PPM2_MAX),
    )


@app.get("/dashboard/geo-points")
def geo_points():
    return fetch_all(
        """
        select lat, lon, price_per_m2
        from marts.current_listings
        where lat is not null and lon is not null
          and price_per_m2 between %s and %s
        order by random() limit 60000
        """,
        (PPM2_MIN, PPM2_MAX),
    )


@app.get("/dashboard/distribution")
def distribution():
    return fetch_all(
        """
        select b as bucket, (100000 + (b - 1) * 50000)::bigint as ppm2_from, count(*) as n
        from (
            select width_bucket(price_per_m2, 100000, 1200000, 22) as b
            from marts.current_listings
            where price_per_m2 between 100000 and 1200000
        ) t
        group by b order by b
        """
    )


@app.get("/listings/hot")
def hot(municipality=Query(None), rooms=Query(None), price_min=Query(None), price_max=Query(None), limit=Query(50)):
    sql = (
        "select cian_id, municipality, rooms, total_area, price, price_per_m2, "
        "nearest_metro, hot_score from marts.hot_listings where 1=1"
    )
    params = []
    if municipality:
        sql += " and municipality = %s"
        params.append(municipality)
    if rooms:
        sql += " and rooms = %s"
        params.append(int(rooms))
    if price_min:
        sql += " and price >= %s"
        params.append(int(price_min))
    if price_max:
        sql += " and price <= %s"
        params.append(int(price_max))
    sql += " order by hot_score desc limit %s"
    params.append(int(limit))
    return fetch_all(sql, tuple(params))


@app.get("/listings/current/export")
def export():
    if not os.path.exists(EXPORT_PATH):
        build_export(get_engine(), EXPORT_PATH)
    return FileResponse(
        EXPORT_PATH,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="current_listings.xlsx",
    )
