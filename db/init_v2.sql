-- схема v2 для cian pipeline
-- чистая установка. миграция с v1: python -m db.migrate_v2

-- живой срез рынка, один ряд на объявление
CREATE TABLE IF NOT EXISTS listings (
    cian_id            BIGINT PRIMARY KEY,
    url                TEXT NOT NULL,
    price              BIGINT NOT NULL,
    price_per_m2       BIGINT,
    deal_conditions    TEXT,
    region             TEXT,
    municipality       TEXT,
    district           TEXT,
    microdistrict      TEXT,
    street             TEXT,
    house              TEXT,
    lat                REAL,
    lon                REAL,
    metro_stations     JSONB,
    rooms              SMALLINT,
    total_area         REAL,
    living_area        REAL,
    kitchen_area       REAL,
    floor              SMALLINT,
    total_floors       SMALLINT,
    ceiling_height     REAL,
    renovation         TEXT,
    bathrooms          TEXT,
    balcony            TEXT,
    window_view        TEXT,
    is_apartments      BOOLEAN,
    year_built         SMALLINT,
    building_type      TEXT,
    parking            TEXT,
    is_new_building    BOOLEAN,
    developer          TEXT,
    residential_complex TEXT,
    completion_date    TEXT,
    description        TEXT,
    publication_date   TEXT,
    seller_type        TEXT,
    phone_protected    BOOLEAN,
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consecutive_misses SMALLINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_listings_active ON listings (is_active) WHERE is_active;
CREATE INDEX IF NOT EXISTS idx_listings_last_seen ON listings (last_seen_at);
CREATE INDEX IF NOT EXISTS idx_listings_coords ON listings (lat, lon) WHERE lat IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_listings_district ON listings (district);
CREATE INDEX IF NOT EXISTS idx_listings_rooms_price ON listings (rooms, price);

-- лог изменений цен, append-only
CREATE TABLE IF NOT EXISTS price_history (
    id           SERIAL PRIMARY KEY,
    cian_id      BIGINT NOT NULL REFERENCES listings(cian_id),
    price        BIGINT NOT NULL,
    price_per_m2 BIGINT,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ph_cian_id ON price_history (cian_id, recorded_at);

-- ежедневные snapshot-ы, партиционирование по месяцам
CREATE TABLE IF NOT EXISTS listings_archive (
    LIKE listings INCLUDING DEFAULTS,
    snapshot_date DATE NOT NULL,
    PRIMARY KEY (cian_id, snapshot_date)
) PARTITION BY RANGE (snapshot_date);

CREATE TABLE IF NOT EXISTS listings_archive_2026_04
    PARTITION OF listings_archive
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

-- исторические данные Kaggle, legacy
CREATE TABLE IF NOT EXISTS kaggle_flats (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    source TEXT NOT NULL,
    price BIGINT NOT NULL,
    price_per_m2 BIGINT,
    discount_pct SMALLINT,
    deal_conditions TEXT,
    city TEXT, region TEXT, district TEXT, street TEXT, house_number TEXT,
    lat REAL, lon REAL,
    metro_stations JSONB, transport_score REAL,
    rooms SMALLINT, total_area REAL, living_area REAL, kitchen_area REAL,
    floor SMALLINT, total_floors SMALLINT, ceiling_height REAL,
    renovation TEXT, bathrooms TEXT, balcony TEXT, window_view TEXT,
    is_apartments BOOLEAN, year_built SMALLINT,
    building_type TEXT, parking TEXT, elevators TEXT,
    is_new_building BOOLEAN, developer TEXT, residential_complex TEXT,
    completion_date TEXT, description TEXT, publication_date TEXT,
    parsed_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (url, source, price)
);
