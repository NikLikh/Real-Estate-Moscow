import traceback
from pathlib import Path

import kagglehub
import pandas as pd
import psycopg2

from db.loader import DB_CONFIG, INSERT_SQL, _build_row


def _transform_mrdaniilak(df: pd.DataFrame) -> pd.DataFrame:
    building_type_map = {
        0: "Другое", 1: "Панельный", 2: "Монолитный",
        3: "Кирпичный", 4: "Блочный", 5: "Деревянный",
    }

    df["is_new_building"] = df["object_type"].map({1: False, 2: True})
    df["building_type"] = df["building_type"].map(building_type_map)
    df = df.rename(columns={
        "date": "publication_date",
        "geo_lat": "lat",
        "geo_lon": "lon",
        "level": "floor",
        "levels": "total_floors",
        "area": "total_area",
    })
    df["source"] = "kaggle_mrdaniilak"
    df["url"] = "kaggle_mrdaniilak_" + df.index.astype(str)
    df["price"] = df["price"].astype("Int64")
    return df


def _transform_egorkainov(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "Price": "price",
        "Number of rooms": "rooms",
        "Area": "total_area",
        "Living area": "living_area",
        "Kitchen area": "kitchen_area",
        "Floor": "floor",
        "Number of floors": "total_floors",
        "Renovation": "renovation",
        "Region": "city",
    })
    df["metro_stations"] = df.apply(
        lambda x: [(x["Metro station"], x["Minutes to metro"])]
        if pd.notna(x.get("Metro station")) else None,
        axis=1,
    )
    df["source"] = "kaggle_egorkainov"
    df["url"] = "kaggle_egorkainov_" + df.index.astype(str)
    return df


def _transform_ivan314sh(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "number_of_floors": "total_floors",
        "construction_year": "year_built",
        "number_of_rooms": "rooms",
        "region_of_moscow": "region",
        "link": "url",
    })
    df["is_new_building"] = df["is_new"].map({"да": True, "нет": False})
    df["is_apartments"] = df["is_apartments"].map({"да": True, "нет": False})
    df["city"] = "Москва"
    df["source"] = "kaggle_ivan314sh"
    df["url"] = "kaggle_ivan314sh_" + df.index.astype(str)
    return df


def _load_df_to_pg(df: pd.DataFrame, label: str):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    inserted, skipped, invalid = 0, 0, 0

    for _, record in df.iterrows():
        row = _build_row(record.to_dict())
        if row.get("price") is None:
            invalid += 1
            continue
        try:
            cursor.execute(INSERT_SQL, row)
            inserted += cursor.rowcount
            if cursor.rowcount == 0:
                skipped += 1
        except Exception:
            conn.rollback()

    conn.commit()
    cursor.close()
    conn.close()
    print(f"[{label}] Загружено {inserted}, дубликатов {skipped}, невалидных {invalid}")


DATASETS = {
    "mrdaniilak/russia-real-estate-20182021": {
        "file": "all_v2.csv",
        "transform": _transform_mrdaniilak,
        "label": "mrdaniilak (2018-2021)",
    },
    "egorkainov/moscow-housing-price-dataset": {
        "file": "moscow_dataset.csv",
        "transform": _transform_egorkainov,
        "label": "egorkainov (2023)",
    },
    "ivan314sh/prices-of-moscow-apartments": {
        "file": "moscow_flats_dataset.csv",
        "transform": _transform_ivan314sh,
        "label": "ivan314sh (2024)",
    },
}


if __name__ == "__main__":
    for dataset_id, config in DATASETS.items():
        print(f"\n{'='*50}")
        print(f"Скачивание: {config['label']}")
        print(f"{'='*50}")

        try:
            path = kagglehub.dataset_download(dataset_id)
            print(f"Скачано → {path}")

            csv_path = Path(path) / config["file"]
            if not csv_path.exists():
                candidates = list(Path(path).rglob("*.csv"))
                if candidates:
                    csv_path = candidates[0]
                    print(f"CSV найден: {csv_path}")
                else:
                    print(f"CSV не найден в {path}")
                    continue

            df = pd.read_csv(csv_path)
            print(f"Строк: {len(df)}, колонок: {list(df.columns)}")

            df = config["transform"](df)
            _load_df_to_pg(df, config["label"])

        except Exception as e:
            print(f"Ошибка: {e}")
            traceback.print_exc()
