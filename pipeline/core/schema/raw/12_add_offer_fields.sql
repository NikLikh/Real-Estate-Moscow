ALTER TABLE raw.cian_observations
    ADD COLUMN IF NOT EXISTS seller_is_owner          BOOLEAN,
    ADD COLUMN IF NOT EXISTS status                   TEXT,
    ADD COLUMN IF NOT EXISTS cian_user_id             BIGINT,
    ADD COLUMN IF NOT EXISTS is_penthouse             BOOLEAN,
    ADD COLUMN IF NOT EXISTS room_type                TEXT,
    ADD COLUMN IF NOT EXISTS demolished_in_renovation BOOLEAN;
