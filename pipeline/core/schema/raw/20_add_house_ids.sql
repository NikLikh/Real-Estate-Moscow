ALTER TABLE raw.cian_observations
    ADD COLUMN IF NOT EXISTS house_id      BIGINT,
    ADD COLUMN IF NOT EXISTS nb_house_id   BIGINT,
    ADD COLUMN IF NOT EXISTS descr_minhash BIGINT[];
