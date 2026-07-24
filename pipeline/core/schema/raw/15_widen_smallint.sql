ALTER TABLE raw.cian_observations
    ALTER COLUMN rooms TYPE INTEGER,
    ALTER COLUMN floor TYPE INTEGER,
    ALTER COLUMN total_floors TYPE INTEGER,
    ALTER COLUMN year_built TYPE INTEGER,
    ALTER COLUMN passenger_lifts TYPE INTEGER,
    ALTER COLUMN cargo_lifts TYPE INTEGER,
    ALTER COLUMN photos_count TYPE INTEGER;
