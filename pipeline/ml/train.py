import json
import os
from datetime import datetime, timezone

import joblib
import lightgbm
import pandas as pd
import sklearn
from lightgbm import LGBMClassifier
from sklearn.compose import ColumnTransformer
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import TargetEncoder

from pipeline.ml.db import make_engine
from pipeline.ml.features import CATEGORICAL, INPUT_COLUMNS, NUMERIC, FeatureBuilder, make_target

TRAIN_SAMPLE = 160_000
HOLDOUT_FRACTION = 0.2

UNIT_COLUMNS = [c for c in INPUT_COLUMNS if c != "days_on_market"]

TRAIN_QUERY = f"""
select {', '.join(INPUT_COLUMNS)} from (
    select distinct on (unit_sk)
        {', '.join(UNIT_COLUMNS)},
        unit_days_on_market as days_on_market,
        last_seen
    from marts.ml_listings_wide
    where unit_closed = 1 and unit_days_on_market >= 0
      and total_area > 0 and price > 0
    order by unit_sk, last_seen desc, cian_id desc
) t
order by last_seen desc
limit {TRAIN_SAMPLE}
"""


def build_pipeline():
    pre = ColumnTransformer([
        ("cat", TargetEncoder(target_type="binary", random_state=0), CATEGORICAL),
        ("num", "passthrough", NUMERIC),
    ])
    model = LGBMClassifier(
        n_estimators=1500, learning_rate=0.03, num_leaves=127,
        subsample=0.8, colsample_bytree=0.8, reg_lambda=1.0, random_state=0
    )
    return Pipeline([("features", FeatureBuilder()), ("pre", pre), ("model", model)])


def train_and_save(checkpoints_dir):
    df = pd.concat(pd.read_sql(TRAIN_QUERY, make_engine(), chunksize=50_000), ignore_index=True)
    y = make_target(df)
    n_te = int(len(df) * HOLDOUT_FRACTION)
    pipe = build_pipeline()
    pipe.fit(df.iloc[n_te:], y.iloc[n_te:])
    proba = pipe.predict_proba(df.iloc[:n_te])[:, 1]
    pipe.fit(df, y)
    metrics = {
        "pr_auc": float(average_precision_score(y.iloc[:n_te], proba)),
        "auc": float(roc_auc_score(y.iloc[:n_te], proba)),
        "n_train": int(len(df)),
        "positive_rate": float(y.mean()),
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "reference_year": int(pipe.named_steps["features"].reference_year_),
        "features": NUMERIC + CATEGORICAL,
        "versions": {
            "sklearn": sklearn.__version__,
            "lightgbm": lightgbm.__version__,
            "pandas": pd.__version__,
        },
    }
    os.makedirs(checkpoints_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    joblib.dump(pipe, os.path.join(checkpoints_dir, f"hot_model_{stamp}.joblib"))
    joblib.dump(pipe, os.path.join(checkpoints_dir, "hot_model_latest.joblib"))
    with open(os.path.join(checkpoints_dir, "hot_model_meta.json"), "w") as fh:
        json.dump(metrics, fh)
    return metrics


def main():
    print(train_and_save(os.path.join(os.path.dirname(__file__), "..", "..", "checkpoints")))


if __name__ == "__main__":
    main()
