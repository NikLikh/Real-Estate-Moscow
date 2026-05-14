-- snapshot-ы live-listings и архивированные неактивные объявления
CREATE TABLE IF NOT EXISTS listings_archive (
    LIKE listings INCLUDING DEFAULTS,
    snapshot_date DATE NOT NULL,
    PRIMARY KEY (cian_id, snapshot_date)
);
