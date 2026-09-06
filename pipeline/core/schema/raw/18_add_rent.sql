ALTER TABLE raw.cian_observations
    ADD COLUMN IF NOT EXISTS deal_type          TEXT,
    ADD COLUMN IF NOT EXISTS deposit            NUMERIC,
    ADD COLUMN IF NOT EXISTS agent_fee          NUMERIC,
    ADD COLUMN IF NOT EXISTS client_fee         NUMERIC,
    ADD COLUMN IF NOT EXISTS prepay_months      NUMERIC,
    ADD COLUMN IF NOT EXISTS lease_term_type    TEXT,
    ADD COLUMN IF NOT EXISTS payment_period     TEXT,
    ADD COLUMN IF NOT EXISTS utilities_included BOOLEAN,
    ADD COLUMN IF NOT EXISTS utilities_price    NUMERIC,
    ADD COLUMN IF NOT EXISTS beds_count         INTEGER,
    ADD COLUMN IF NOT EXISTS pets_allowed       BOOLEAN,
    ADD COLUMN IF NOT EXISTS children_allowed   BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_fridge         BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_washer         BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_dishwasher     BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_conditioner    BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_tv             BOOLEAN,
    ADD COLUMN IF NOT EXISTS has_internet       BOOLEAN;

CREATE INDEX IF NOT EXISTS idx_obs_rent
    ON raw.cian_observations (deal_type, cian_id)
    WHERE deal_type IN ('rent_long', 'rent_day');
