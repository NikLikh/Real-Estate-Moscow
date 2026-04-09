-- Silver + Gold таблицы для medallion-архитектуры
-- запускать после db/init_v2.sql

-- единый timeline 2018-2026, одна строка на квартиру (дедуплицировано)
CREATE TABLE IF NOT EXISTS silver_listings (
    listing_id          BIGSERIAL PRIMARY KEY,
    cian_id             BIGINT,
    source              TEXT NOT NULL,
    url                 TEXT,

    -- группа дубликатов
    dedup_group_id      BIGINT,
    is_primary          BOOLEAN NOT NULL DEFAULT TRUE,
    group_size          SMALLINT DEFAULT 1,
    group_min_price     BIGINT,
    group_max_price     BIGINT,
    group_seller_types  TEXT,

    -- цена
    price               BIGINT NOT NULL,
    price_per_m2        BIGINT NOT NULL,

    -- адрес (raw)
    city                TEXT,
    region              TEXT,
    municipality        TEXT,
    district            TEXT,
    microdistrict       TEXT,
    street              TEXT,
    house               TEXT,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,

    -- адрес (обогащённый)
    okrug               TEXT,
    raion               TEXT,
    nearest_metro       TEXT,
    metro_distance_m    REAL,
    metro_walk_min      REAL,
    dist_to_center_km   REAL,
    metro_stations      TEXT,

    -- параметры квартиры
    rooms               SMALLINT,
    total_area          REAL NOT NULL,
    living_area         REAL,
    kitchen_area        REAL,
    floor               SMALLINT,
    total_floors        SMALLINT,
    ceiling_height      REAL,

    -- вычисляемые
    floor_ratio         REAL,
    living_ratio        REAL,
    kitchen_ratio       REAL,

    -- здание
    building_type       TEXT,
    year_built          SMALLINT,
    year_built_source   TEXT,
    building_era        TEXT,
    renovation          TEXT,
    bathrooms           TEXT,
    balcony             TEXT,
    window_view         TEXT,
    parking             TEXT,
    is_apartments       BOOLEAN,

    -- новостройка
    is_new_building     BOOLEAN,
    developer           TEXT,
    residential_complex TEXT,
    completion_date     TEXT,
    completion_year     SMALLINT,
    stage               SMALLINT,

    -- время
    publication_date    DATE,
    pub_year            SMALLINT,
    pub_month           TEXT,
    pub_quarter         TEXT,
    date_source         TEXT,

    -- lifecycle (только cian)
    seller_type         TEXT,
    is_active           BOOLEAN,
    first_seen_at       TIMESTAMPTZ,
    last_seen_at        TIMESTAMPTZ,

    -- quality
    has_coords          BOOLEAN DEFAULT FALSE,
    has_year_built      BOOLEAN DEFAULT FALSE,
    has_pub_date        BOOLEAN DEFAULT FALSE,
    data_quality_score  SMALLINT DEFAULT 0,

    -- etl
    etl_loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_version         TEXT NOT NULL DEFAULT '1.0'
);

CREATE INDEX IF NOT EXISTS idx_silver_primary ON silver_listings (is_primary) WHERE is_primary;
CREATE INDEX IF NOT EXISTS idx_silver_dedup ON silver_listings (dedup_group_id);
CREATE INDEX IF NOT EXISTS idx_silver_coords ON silver_listings (lat, lon) WHERE lat IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_pub_month ON silver_listings (pub_month);
CREATE INDEX IF NOT EXISTS idx_silver_okrug ON silver_listings (okrug);
CREATE INDEX IF NOT EXISTS idx_silver_source ON silver_listings (source);
CREATE INDEX IF NOT EXISTS idx_silver_cian ON silver_listings (cian_id) WHERE cian_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_silver_new_building ON silver_listings (is_new_building) WHERE is_new_building;

-- помесячная медиана цен по зданиям
CREATE TABLE IF NOT EXISTS gold_building_monthly (
    building_id         BIGINT NOT NULL,
    lat4                NUMERIC(8, 4),
    lon4                NUMERIC(8, 4),
    pub_month           TEXT NOT NULL,
    med_ppm2            BIGINT NOT NULL,
    avg_ppm2            BIGINT,
    min_ppm2            BIGINT,
    max_ppm2            BIGINT,
    n_obs               INT NOT NULL,
    n_sources           SMALLINT,
    okrug               TEXT,
    raion               TEXT,
    building_type       TEXT,
    building_era        TEXT,
    is_new_building     BOOLEAN,
    total_floors        SMALLINT,
    year_built          SMALLINT,
    nearest_metro       TEXT,
    PRIMARY KEY (building_id, pub_month)
);

-- доходности по зданиям на разных горизонтах
CREATE TABLE IF NOT EXISTS gold_building_returns (
    building_id         BIGINT NOT NULL,
    lat4                NUMERIC(8, 4),
    lon4                NUMERIC(8, 4),
    start_month         TEXT NOT NULL,
    horizon_months      SMALLINT NOT NULL,
    start_ppm2          BIGINT NOT NULL,
    end_ppm2            BIGINT NOT NULL,
    return_pct          REAL NOT NULL,
    annualized_return   REAL,
    okrug               TEXT,
    raion               TEXT,
    is_new_building     BOOLEAN,
    PRIMARY KEY (building_id, start_month, horizon_months)
);

-- ценовые индексы по сегментам
CREATE TABLE IF NOT EXISTS gold_segment_index (
    segment_key         TEXT NOT NULL,
    okrug               TEXT,
    is_new_building     BOOLEAN,
    rooms_bucket        TEXT,
    pub_month           TEXT NOT NULL,
    raw_med_ppm2        BIGINT,
    hedonic_index       REAL,
    hedonic_ppm2        BIGINT,
    n_obs               INT NOT NULL,
    ppm2_q25            BIGINT,
    ppm2_q75            BIGINT,
    ppm2_std            REAL,
    PRIMARY KEY (segment_key, pub_month)
);

-- доходность по стадии стройки
CREATE TABLE IF NOT EXISTS gold_stage_returns (
    stage               SMALLINT NOT NULL,
    horizon_months      SMALLINT NOT NULL,
    entry_year          SMALLINT NOT NULL DEFAULT 0,
    med_return_pct      REAL NOT NULL,
    avg_return_pct      REAL,
    q25_return_pct      REAL,
    q75_return_pct      REAL,
    pct_positive        REAL,
    sharpe_like         REAL,
    n_obs               INT NOT NULL,
    PRIMARY KEY (stage, horizon_months, entry_year)
);

-- готовая feature-таблица для ML
CREATE TABLE IF NOT EXISTS gold_ml_features (
    feature_id          BIGSERIAL PRIMARY KEY,
    building_id         BIGINT NOT NULL,
    start_month         TEXT NOT NULL,
    relative_return_12m REAL,
    relative_return_24m REAL,
    above_median        BOOLEAN,
    rooms_mode          SMALLINT,
    total_area_med      REAL,
    total_floors        SMALLINT,
    building_type       TEXT,
    building_era        TEXT,
    year_built          SMALLINT,
    renovation_mode     TEXT,
    is_apartments_pct   REAL,
    okrug               TEXT,
    raion               TEXT,
    dist_to_center_km   REAL,
    metro_walk_min      REAL,
    nearest_metro       TEXT,
    is_new_building     BOOLEAN,
    stage               SMALLINT,
    developer           TEXT,
    segment_ppm2        BIGINT,
    segment_ppm2_chg_3m  REAL,
    segment_ppm2_chg_6m  REAL,
    segment_ppm2_chg_12m REAL,
    building_ppm2       BIGINT,
    ppm2_vs_segment     REAL,
    cbr_rate            REAL,
    mortgage_rate       REAL,
    mortgage_volume     REAL,
    usd_rub             REAL,
    cpi_yoy             REAL,
    cbr_rate_delta_3m   REAL,
    pub_year            SMALLINT,
    pub_quarter         SMALLINT,
    pub_month_num       SMALLINT,
    n_obs_building      INT,
    data_quality_score  SMALLINT
);

-- агрегаты по районам для дашбордов
CREATE TABLE IF NOT EXISTS gold_district_stats (
    okrug               TEXT NOT NULL,
    raion               TEXT NOT NULL DEFAULT '',
    is_new_building     BOOLEAN NOT NULL,
    pub_month           TEXT NOT NULL,
    n_listings          INT NOT NULL,
    n_buildings         INT NOT NULL,
    med_ppm2            BIGINT,
    avg_ppm2            BIGINT,
    ppm2_q25            BIGINT,
    ppm2_q75            BIGINT,
    med_price           BIGINT,
    ppm2_chg_1m         REAL,
    ppm2_chg_3m         REAL,
    ppm2_chg_12m        REAL,
    avg_area            REAL,
    avg_rooms           REAL,
    pct_studio          REAL,
    pct_1room           REAL,
    pct_2room           REAL,
    pct_3plus           REAL,
    PRIMARY KEY (okrug, raion, is_new_building, pub_month)
);

-- справочник координат для геообогащения
CREATE TABLE IF NOT EXISTS dim_geo_coords (
    lat4                NUMERIC(8, 4) NOT NULL,
    lon4                NUMERIC(8, 4) NOT NULL,
    okrug               TEXT,
    raion               TEXT,
    nearest_metro       TEXT,
    metro_distance_m    REAL,
    metro_walk_min      REAL,
    dist_to_center_km   REAL,
    PRIMARY KEY (lat4, lon4)
);
