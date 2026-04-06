# идемпотентная миграция v1 (flats) в v2 (listings + price_history + archive)
# запуск: python -m db.migrate_v2
# просмотр: python -m db.migrate_v2 --dry-run

import logging
import sys

from db.connection import get_conn, put_conn

log = logging.getLogger("re")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")


CREATE_LISTINGS = """
CREATE TABLE IF NOT EXISTS listings (
    cian_id            BIGINT PRIMARY KEY,
    url                TEXT NOT NULL,
    price              BIGINT NOT NULL,
    price_per_m2       BIGINT,
    discount_pct       SMALLINT,
    deal_conditions    TEXT,
    city               TEXT,
    region             TEXT,
    district           TEXT,
    street             TEXT,
    house_number       TEXT,
    lat                REAL,
    lon                REAL,
    metro_stations     JSONB,
    transport_score    REAL,
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
    elevators          TEXT,
    is_new_building    BOOLEAN,
    developer          TEXT,
    residential_complex TEXT,
    completion_date    TEXT,
    description        TEXT,
    publication_date   TEXT,
    seller_type        TEXT,
    photos_count       SMALLINT,
    views_count        INT,
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
"""

CREATE_PRICE_HISTORY = """
CREATE TABLE IF NOT EXISTS price_history (
    id           SERIAL PRIMARY KEY,
    cian_id      BIGINT NOT NULL REFERENCES listings(cian_id),
    price        BIGINT NOT NULL,
    price_per_m2 BIGINT,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ph_cian_id ON price_history (cian_id, recorded_at);
"""

CREATE_ARCHIVE = """
CREATE TABLE IF NOT EXISTS listings_archive (
    LIKE listings INCLUDING DEFAULTS,
    snapshot_date DATE NOT NULL,
    PRIMARY KEY (cian_id, snapshot_date)
) PARTITION BY RANGE (snapshot_date);
"""

CREATE_ARCHIVE_PARTITION = """
CREATE TABLE IF NOT EXISTS listings_archive_2026_04
    PARTITION OF listings_archive
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
"""

# берем последнюю запись на каждый cian_id (по parsed_at DESC)
MIGRATE_CIAN = """
INSERT INTO listings (
    cian_id, url, price, price_per_m2, discount_pct, deal_conditions,
    city, region, district, street, house_number, lat, lon,
    metro_stations, transport_score,
    rooms, total_area, living_area, kitchen_area,
    floor, total_floors, ceiling_height,
    renovation, bathrooms, balcony, window_view, is_apartments,
    year_built, building_type, parking, elevators,
    is_new_building, developer, residential_complex, completion_date,
    description, publication_date,
    is_active, first_seen_at, last_seen_at, updated_at, consecutive_misses
)
SELECT DISTINCT ON ((regexp_match(url, '/flat/(\\d+)'))[1]::BIGINT)
    (regexp_match(url, '/flat/(\\d+)'))[1]::BIGINT,
    split_part(url, '?', 1),
    price, price_per_m2, discount_pct, deal_conditions,
    city, region, district, street, house_number, lat, lon,
    metro_stations, transport_score,
    rooms, total_area, living_area, kitchen_area,
    floor, total_floors, ceiling_height,
    renovation, bathrooms, balcony, window_view, is_apartments,
    year_built, building_type, parking, elevators,
    is_new_building, developer, residential_complex, completion_date,
    description, publication_date,
    TRUE, parsed_at, parsed_at, parsed_at, 0
FROM {src}
WHERE source = 'cian' AND url ~ '/flat/\\d+'
ORDER BY (regexp_match(url, '/flat/(\\d+)'))[1]::BIGINT, parsed_at DESC
ON CONFLICT (cian_id) DO NOTHING;
"""

# правим first_seen_at на самое раннее наблюдение
FIX_FIRST_SEEN = """
UPDATE listings l SET first_seen_at = sub.min_at
FROM (
    SELECT (regexp_match(url, '/flat/(\\d+)'))[1]::BIGINT AS cid,
           MIN(parsed_at) AS min_at
    FROM {src}
    WHERE source = 'cian' AND url ~ '/flat/\\d+'
    GROUP BY (regexp_match(url, '/flat/(\\d+)'))[1]::BIGINT
) sub
WHERE l.cian_id = sub.cid AND l.first_seen_at != sub.min_at;
"""

MIGRATE_PRICE_HISTORY = """
INSERT INTO price_history (cian_id, price, price_per_m2, recorded_at)
SELECT
    (regexp_match(url, '/flat/(\\d+)'))[1]::BIGINT,
    price, price_per_m2, COALESCE(parsed_at, NOW())
FROM {src}
WHERE source = 'cian_history' AND url ~ '/flat/\\d+'
  AND (regexp_match(url, '/flat/(\\d+)'))[1]::BIGINT IN (SELECT cian_id FROM listings);
"""

SEED_INITIAL_PRICES = """
INSERT INTO price_history (cian_id, price, price_per_m2, recorded_at)
SELECT cian_id, price, price_per_m2, first_seen_at
FROM listings l
WHERE NOT EXISTS (SELECT 1 FROM price_history ph WHERE ph.cian_id = l.cian_id);
"""

CLEANUP_KAGGLE = "DELETE FROM kaggle_flats WHERE source NOT LIKE 'kaggle%%';"


def _table_exists(cur, name):
    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
        (name,),
    )
    return cur.fetchone()[0]


def _count(cur, table):
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


def migrate(dry_run=False):
    conn = get_conn()
    try:
        cur = conn.cursor()

        # создаем таблицы
        log.info("step 1: creating listings table")
        cur.execute(CREATE_LISTINGS)

        log.info("step 2: creating price_history table")
        cur.execute(CREATE_PRICE_HISTORY)

        log.info("step 3: creating listings_archive")
        if not _table_exists(cur, "listings_archive"):
            cur.execute(CREATE_ARCHIVE)
        if not _table_exists(cur, "listings_archive_2026_04"):
            cur.execute(CREATE_ARCHIVE_PARTITION)

        # переименовываем flats в kaggle_flats
        if _table_exists(cur, "flats"):
            log.info("step 4: переименовываем flats в kaggle_flats")
            if not dry_run:
                cur.execute("ALTER TABLE flats RENAME TO kaggle_flats")
                src = "kaggle_flats"
            else:
                src = "flats"
        elif _table_exists(cur, "kaggle_flats"):
            log.info("step 4: flats already renamed, skipping")
            src = "kaggle_flats"
        else:
            log.warning("step 4: no flats or kaggle_flats table found, skipping data migration")
            if not dry_run:
                conn.commit()
            cur.close()
            return

        # переносим cian-записи в listings
        cur.execute(
            f"SELECT COUNT(*) FROM {src} WHERE source = 'cian' AND url ~ '/flat/\\d+'"
        )
        cian_count = cur.fetchone()[0]
        log.info(f"step 5: migrating {cian_count} cian rows into listings")
        if not dry_run:
            cur.execute(MIGRATE_CIAN.format(src=src))
            log.info(f"  inserted {cur.rowcount} listings")
            cur.execute(FIX_FIRST_SEEN.format(src=src))
            log.info(f"  fixed first_seen_at for {cur.rowcount} rows")

        # переносим историю цен
        cur.execute(
            f"SELECT COUNT(*) FROM {src} WHERE source = 'cian_history' AND url ~ '/flat/\\d+'"
        )
        hist_count = cur.fetchone()[0]
        log.info(f"step 6: migrating {hist_count} price history rows")
        if not dry_run:
            ph_before = _count(cur, "price_history")
            if ph_before == 0:
                cur.execute(MIGRATE_PRICE_HISTORY.format(src=src))
                log.info(f"  inserted {cur.rowcount} history rows")
                cur.execute(SEED_INITIAL_PRICES)
                log.info(f"  seeded {cur.rowcount} initial prices")
            else:
                log.info(f"  price_history already has {ph_before} rows, skipping")

        # чистим kaggle_flats от не-kaggle записей
        cur.execute(f"SELECT COUNT(*) FROM {src} WHERE source NOT LIKE 'kaggle%%'")
        non_kaggle = cur.fetchone()[0]
        log.info(f"step 7: removing {non_kaggle} non-kaggle rows from kaggle_flats")
        if not dry_run:
            cur.execute(CLEANUP_KAGGLE)
            log.info(f"  deleted {cur.rowcount} rows")

        # итоги
        if not dry_run:
            conn.commit()
            log.info(
                f"migration complete: listings={_count(cur, 'listings')}, "
                f"price_history={_count(cur, 'price_history')}, "
                f"kaggle_flats={_count(cur, 'kaggle_flats')}"
            )
        else:
            conn.rollback()
            log.info("dry run complete, no changes made")

        cur.close()
    except Exception as e:
        conn.rollback()
        log.error(f"migration failed: {e}")
        raise
    finally:
        put_conn(conn)


if __name__ == "__main__":
    migrate(dry_run="--dry-run" in sys.argv)
