import logging
import traceback
from pathlib import Path

import kagglehub
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    lit,
    monotonically_increasing_id,
    regexp_extract,
    when,
)

from config.settings import JARS_PATH, JDBC_PROPS, JDBC_URL
from pipeline.core.connection import get_conn, put_conn
from pipeline.kaggle.repository import INSERT_SQL, build_row

log = logging.getLogger("re")

SCHEMA = {
    "url": "string",
    "source": "string",
    "price": "long",
    "price_per_m2": "long",
    "discount_pct": "short",
    "deal_conditions": "string",
    "city": "string",
    "region": "string",
    "district": "string",
    "street": "string",
    "house_number": "string",
    "lat": "float",
    "lon": "float",
    "transport_score": "float",
    "rooms": "short",
    "total_area": "float",
    "living_area": "float",
    "kitchen_area": "float",
    "floor": "short",
    "total_floors": "short",
    "ceiling_height": "float",
    "renovation": "string",
    "bathrooms": "string",
    "balcony": "string",
    "window_view": "string",
    "is_apartments": "boolean",
    "year_built": "short",
    "building_type": "string",
    "parking": "string",
    "elevators": "string",
    "is_new_building": "boolean",
    "developer": "string",
    "residential_complex": "string",
    "completion_date": "string",
    "description": "string",
    "publication_date": "string",
}

FINAL_COLUMNS = list(SCHEMA.keys())

BUILDING_TYPE_MAP = {
    1: "Панельный",
    2: "Монолитный",
    3: "Кирпичный",
    4: "Блочный",
    5: "Деревянный",
}


def _building_type_expr():
    expr = lit(None).cast("string")
    for k, v in BUILDING_TYPE_MAP.items():
        expr = when(col("building_type") == k, v).otherwise(expr)
    return expr


def _create_spark():
    return (
        SparkSession.builder.appName("kaggle_loader")
        .master("local[4]")
        .config("spark.jars", JARS_PATH)
        .config("spark.driver.extraClassPath", JARS_PATH)
        .config("spark.executor.extraClassPath", JARS_PATH)
        .config("spark.driver.memory", "6g")
        .getOrCreate()
    )


def _align_columns(df):
    for col_name, col_type in SCHEMA.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast(col_type))
        else:
            df = df.withColumn(col_name, col(col_name).cast(col_type))
    return df.select(FINAL_COLUMNS)


def _write_to_pg(df, label):
    df = df.dropDuplicates(["url", "source", "price"])
    log.info(f"[{label}] writing to PG...")
    (
        df.write.mode("append")
        .option("batchsize", 5000)
        .option("numPartitions", 2)
        .jdbc(JDBC_URL, "raw.kaggle_flats", properties=JDBC_PROPS)
    )
    log.info(f"[{label}] done")


def _transform_mrdaniilak(df):
    return _align_columns(
        df.filter(col("region").isin(81, 3))
        .withColumn(
            "city", when(col("region") == 81, "Москва").otherwise("Московская область")
        )
        .withColumn("region", lit(None).cast("string"))
        .withColumnRenamed("date", "publication_date")
        .withColumnRenamed("geo_lat", "lat")
        .withColumnRenamed("geo_lon", "lon")
        .withColumnRenamed("level", "floor")
        .withColumnRenamed("levels", "total_floors")
        .withColumnRenamed("area", "total_area")
        .filter(col("lat").between(54.2, 57.0) & col("lon").between(35.0, 40.5))
        .filter(col("price") > 0)
        .filter(col("total_area").isNull() | col("total_area").between(5, 500))
        .withColumn("rooms", when(col("rooms") < 0, None).otherwise(col("rooms")))
        .withColumn(
            "is_new_building", when(col("object_type") == 2, True).otherwise(False)
        )
        .withColumn("building_type", _building_type_expr())
        .withColumn("price", col("price").cast("long"))
        .withColumn("source", lit("kaggle_mrdaniilak"))
        .withColumn(
            "url",
            concat(
                lit("kaggle_mrdaniilak_"), monotonically_increasing_id().cast("string")
            ),
        )
        .filter(col("price").isNotNull())
    )


def _transform_egorkainov(df):
    renovation_map = {
        "Cosmetic": "Косметический",
        "European-style renovation": "Евроремонт",
        "Without renovation": "Без ремонта",
        "Designer renovation": "Дизайнерский",
        "Designer": "Дизайнерский",
    }
    reno_expr = col("Renovation")
    for eng, rus in renovation_map.items():
        reno_expr = when(col("Renovation") == eng, lit(rus)).otherwise(reno_expr)

    return _align_columns(
        df.withColumnRenamed("Price", "price")
        .withColumnRenamed("Number of rooms", "rooms")
        .withColumnRenamed("Area", "total_area")
        .withColumnRenamed("Living area", "living_area")
        .withColumnRenamed("Kitchen area", "kitchen_area")
        .withColumnRenamed("Floor", "floor")
        .withColumnRenamed("Number of floors", "total_floors")
        .withColumn("renovation", reno_expr)
        .withColumn(
            "city",
            when(col("Region") == "Moscow", "Москва")
            .when(col("Region") == "Moscow region", "Московская область")
            .otherwise(col("Region")),
        )
        .withColumn("price", col("price").cast("long"))
        .withColumn("source", lit("kaggle_egorkainov"))
        .withColumn(
            "url",
            concat(
                lit("kaggle_egorkainov_"), monotonically_increasing_id().cast("string")
            ),
        )
        .filter(col("price").isNotNull())
    )


def _transform_ivan314sh(df):
    return _align_columns(
        df.withColumnRenamed("number_of_floors", "total_floors")
        .withColumnRenamed("construction_year", "year_built")
        .withColumnRenamed("number_of_rooms", "rooms")
        .withColumnRenamed("region_of_moscow", "region")
        .withColumnRenamed("link", "url")
        .withColumn("is_new_building", when(col("is_new") == 1, True).otherwise(False))
        .withColumn(
            "is_apartments",
            when(col("is_apartments").isNull(), None)
            .when(col("is_apartments") == 1, True)
            .otherwise(False),
        )
        .withColumn("price", col("price").cast("long"))
        .withColumn("city", lit("Москва"))
        .withColumn("source", lit("kaggle_ivan314sh"))
        .filter(col("price").isNotNull())
    )


def _transform_mrdaniilak_2021(df):
    return _align_columns(
        df.filter(col("id_region").isin(77, 50))
        .withColumn(
            "city",
            when(col("id_region") == 77, "Москва").otherwise("Московская область"),
        )
        .withColumnRenamed("geo_lat", "lat")
        .withColumnRenamed("geo_lon", "lon")
        .withColumnRenamed("level", "floor")
        .withColumnRenamed("levels", "total_floors")
        .withColumnRenamed("area", "total_area")
        .filter(col("lat").between(54.2, 57.0) & col("lon").between(35.0, 40.5))
        .filter(col("price") > 0)
        .filter(col("total_area").isNull() | col("total_area").between(5, 500))
        .withColumn("rooms", when(col("rooms") < 0, None).otherwise(col("rooms")))
        .withColumn("building_type", _building_type_expr())
        .withColumn(
            "is_new_building", when(col("object_type") == 2, True).otherwise(False)
        )
        .withColumn("price", col("price").cast("long"))
        .withColumn("source", lit("kaggle_mrdaniilak_2021"))
        .withColumn(
            "url",
            concat(
                lit("kaggle_mrdaniilak_2021_"),
                monotonically_increasing_id().cast("string"),
            ),
        )
        .filter(col("price").isNotNull())
    )


def _transform_romanbaster(df):
    floor_str = regexp_extract(col("level"), r"^(\d+)", 1)
    floors_str = regexp_extract(col("level"), r"/(\d+)", 1)
    return _align_columns(
        df.filter(col("city") == "Москва")
        .filter(col("price") > 0)
        .withColumn("floor", when(floor_str != "", floor_str.cast("short")))
        .withColumn("total_floors", when(floors_str != "", floors_str.cast("short")))
        .withColumnRenamed("area", "total_area")
        .withColumnRenamed("material", "building_type")
        .withColumn(
            "building_type",
            when(col("building_type") == "0", None)
            .when(col("building_type") == "Монолит", "Монолитный")
            .when(col("building_type") == "Панель", "Панельный")
            .when(col("building_type") == "Кирпич", "Кирпичный")
            .when(col("building_type") == "Блоки", "Блочный")
            .when(col("building_type") == "Дерево", "Деревянный")
            .otherwise(col("building_type")),
        )
        .withColumnRenamed("price_by_meter", "price_per_m2")
        .withColumnRenamed("longitude", "lon")
        .withColumnRenamed("latitude", "lat")
        .withColumnRenamed("published", "publication_date")
        .withColumn(
            "year_built",
            when(col("build_year") == 0, None).otherwise(col("build_year")),
        )
        .withColumn(
            "is_new_building",
            when(col("object_type") == "Новостройка", True).otherwise(False),
        )
        .withColumn("price", col("price").cast("long"))
        .withColumn("price_per_m2", col("price_per_m2").cast("long"))
        .withColumn("city", lit("Москва"))
        .withColumn("source", lit("kaggle_romanbaster"))
        .withColumn(
            "url",
            concat(
                lit("kaggle_romanbaster_"),
                monotonically_increasing_id().cast("string"),
            ),
        )
        .filter(col("price").isNotNull())
    )


def _transform_hishamhaydar(df):
    old_cols = df.columns
    unique_names = [f"c{i}_{c}" for i, c in enumerate(old_cols)]
    df = df.toDF(*unique_names)

    df = df.select(
        col(unique_names[3]).alias("Price"),
        col(unique_names[2]).alias("Rooms"),
        col(unique_names[4]).alias("Totsp"),
        col(unique_names[5]).alias("Livesp"),
        col(unique_names[6]).alias("Kitsp"),
        col(unique_names[16]).alias("NFloor"),
        col(unique_names[15]).alias("Floors"),
        col(unique_names[14]).alias("New"),
        col(unique_names[10]).alias("Brick"),
        col(unique_names[12]).alias("Bal"),
    )
    return _align_columns(
        df.withColumnRenamed("Price", "price")
        .withColumnRenamed("Rooms", "rooms")
        .withColumnRenamed("Totsp", "total_area")
        .withColumnRenamed("Livesp", "living_area")
        .withColumnRenamed("Kitsp", "kitchen_area")
        .withColumnRenamed("Floors", "total_floors")
        .withColumnRenamed("NFloor", "floor")
        .withColumn("is_new_building", when(col("New") == 1, True).otherwise(False))
        .withColumn(
            "building_type",
            when(col("Brick") == 1, "Кирпичный").otherwise(None),
        )
        .withColumn(
            "balcony",
            when(col("Bal") == 1, "Есть").otherwise(None),
        )
        .withColumn("price", col("price").cast("long"))
        .withColumn("city", lit("Москва"))
        .withColumn("source", lit("kaggle_hishamhaydar"))
        .withColumn(
            "url",
            concat(
                lit("kaggle_hishamhaydar_"),
                monotonically_increasing_id().cast("string"),
            ),
        )
        .filter(col("price").isNotNull())
    )


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
    "mrdaniilak/russia-real-estate-2021": {
        "file": "input_data.csv",
        "sep": ";",
        "transform": _transform_mrdaniilak_2021,
        "label": "mrdaniilak (2021)",
        "source_check": "kaggle_mrdaniilak_2021",
    },
    "romanbaster/sale-and-rental-of-russian-real-estate-in-4-cities": {
        "file": "selling_apartments.csv",
        "transform": _transform_romanbaster,
        "label": "romanbaster (2020)",
    },
    "hishamhaydar/moscow-2018-housing-prices": {
        "file": "2_5393538523506672609.xlsx",
        "xlsx_sheet": "data",
        "transform": _transform_hishamhaydar,
        "label": "hishamhaydar (2018)",
    },
}


def _load_xlsx_via_pandas(path, config):
    import pandas as pd

    df = pd.read_excel(path, sheet_name=config["xlsx_sheet"])
    log.info(f"[{config['label']}] rows: {len(df)}")

    df = df.rename(
        columns={
            "Price": "price",
            "Rooms": "rooms",
            "Totsp": "total_area",
            "Livesp": "living_area",
            "Kitsp": "kitchen_area",
            "NFloor": "floor",
            "Floors": "total_floors",
        }
    )
    df["is_new_building"] = df["New"].fillna(0).astype(bool)
    df["building_type"] = df["Brick"].map({1: "Кирпичный"})
    df["balcony"] = df["Bal"].map({1: "Есть"})
    df["city"] = "Москва"
    df["source"] = "kaggle_hishamhaydar"
    df["url"] = "kaggle_hishamhaydar_" + df.index.astype(str)
    df["price"] = df["price"].astype("Int64")

    conn = get_conn()
    try:
        cur = conn.cursor()
        ok, inv = 0, 0
        for _, r in df.iterrows():
            row = build_row(r.to_dict())
            if not row.get("price"):
                inv += 1
                continue
            try:
                cur.execute(INSERT_SQL, row)
                ok += cur.rowcount
            except Exception:
                conn.rollback()
        conn.commit()
        cur.close()
        log.info(f"[{config['label']}] loaded {ok}, invalid {inv}")
    finally:
        put_conn(conn)


def _get_loaded_sources():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT source FROM raw.kaggle_flats WHERE source LIKE 'kaggle_%'")
        sources = {row[0] for row in cur.fetchall()}
        cur.close()
        return sources
    finally:
        put_conn(conn)


def main():
    spark = _create_spark()
    loaded = _get_loaded_sources()
    log.info(f"already loaded: {loaded or 'nothing'}")

    for dataset_id, config in DATASETS.items():
        source_name = config.get("source_check", f"kaggle_{dataset_id.split('/')[0]}")
        if source_name in loaded:
            log.info(f"[{config['label']}] already in DB, skip")
            continue

        log.info(f"{'='*50}")
        log.info(config["label"])

        try:
            path = kagglehub.dataset_download(dataset_id)
            csv_path = Path(path) / config["file"]
            if not csv_path.exists():
                candidates = list(Path(path).rglob("*.csv"))
                csv_path = candidates[0] if candidates else None
            if not csv_path:
                log.warning("CSV not found")
                continue

            if "xlsx_sheet" in config:
                _load_xlsx_via_pandas(str(csv_path), config)
            else:
                sep = config.get("sep", ",")
                df = spark.read.csv(
                    str(csv_path), header=True, inferSchema=True, sep=sep
                )
                df = config["transform"](df)
                _write_to_pg(df, config["label"])

        except Exception as e:
            log.error(f"error: {e}")
            traceback.print_exc()

    spark.stop()


if __name__ == "__main__":
    main()
