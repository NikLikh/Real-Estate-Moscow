import os

import joblib
import pandas as pd
from sqlalchemy import text

from pipeline.ml.db import make_engine
from pipeline.ml.export import build_export

SCORE_QUERY = """
select * from marts.ml_listings_wide
where event_closed = 0 and total_area > 0 and price > 0
"""
HOT_COLUMNS = ["cian_id", "municipality", "rooms", "total_area", "price",
               "price_per_m2", "nearest_metro", "hot_score"]


def _write_hot_listings(df, engine):
    with engine.begin() as conn:
        df.to_sql("hot_listings_tmp", conn, schema="marts", if_exists="replace", index=False)
        conn.execute(text("drop table if exists marts.hot_listings"))
        conn.execute(text("alter table marts.hot_listings_tmp rename to hot_listings"))
        conn.execute(text(
            "create index if not exists hot_listings_muni_idx on marts.hot_listings (municipality)"))


def score_active(checkpoints_dir, engine):
    df = pd.read_sql(SCORE_QUERY, engine)
    pipe = joblib.load(os.path.join(checkpoints_dir, "hot_model_latest.joblib"))
    df["hot_score"] = pipe.predict_proba(df)[:, 1]
    df["price_per_m2"] = (pd.to_numeric(df["price"], errors="coerce")
                          / pd.to_numeric(df["total_area"], errors="coerce")).round()
    out = df[HOT_COLUMNS].copy()
    _write_hot_listings(out, engine)
    print(f"scored={len(out)} avg_hot_score={out['hot_score'].mean():.4f} "
          f"hot_ge_05={(out['hot_score'] >= 0.5).sum()}")
    return len(out)


def main():
    engine = make_engine()
    ckpt = os.path.join(os.path.dirname(__file__), "..", "..", "checkpoints")
    n = score_active(ckpt, engine)
    build_export(engine, os.path.join(ckpt, "current_listings.xlsx"))
    print(n)


if __name__ == "__main__":
    main()
