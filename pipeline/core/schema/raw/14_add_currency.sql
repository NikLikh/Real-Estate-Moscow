ALTER TABLE raw.cian_observations
    ADD COLUMN IF NOT EXISTS currency TEXT;
