-- append-only лог изменений цен по cian-объявлениям
-- FK на listings не ставим: данные должны переживать архивацию объявлений
CREATE TABLE IF NOT EXISTS price_history (
    id           SERIAL PRIMARY KEY,
    cian_id      BIGINT NOT NULL,
    price        BIGINT NOT NULL,
    price_per_m2 BIGINT,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ph_cian_id ON price_history (cian_id, recorded_at);
