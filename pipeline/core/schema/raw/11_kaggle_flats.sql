CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.kaggle_flats (
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
