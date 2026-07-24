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
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import TargetEncoder

from pipeline.ml.db import make_engine
from pipeline.ml.features import CATEGORICAL, INPUT_COLUMNS, NUMERIC, FeatureBuilder, make_target

TRAIN_QUERY = f"""
select {', '.join(INPUT_COLUMNS)} from marts.ml_listings_wide
where event_closed = 1 and days_on_market >= 0
  and total_area > 0 and price > 0
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
    df = pd.read_sql(TRAIN_QUERY, make_engine())
    y = make_target(df)
    x_tr, x_te, y_tr, y_te = train_test_split(df, y, test_size=0.2, random_state=0, stratify=y)
    pipe = build_pipeline()
    pipe.fit(x_tr, y_tr)
    proba = pipe.predict_proba(x_te)[:, 1]
    metrics = {
        "pr_auc": float(average_precision_score(y_te, proba)),
        "auc": float(roc_auc_score(y_te, proba)),
        "n_train": int(len(x_tr)),
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
