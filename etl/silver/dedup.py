import logging

log = logging.getLogger("re")

TBL = "silver_staging"


def dedup(cur):
    """дедупликация через window functions в PostgreSQL"""

    # колонки для дедупликации
    for col, typ in [
        ("dedup_group_id", "bigint"),
        ("is_primary", "boolean DEFAULT true"),
        ("group_size", "smallint DEFAULT 1"),
        ("group_min_price", "bigint"),
        ("group_max_price", "bigint"),
        ("group_seller_types", "text"),
    ]:
        cur.execute(f"ALTER TABLE {TBL} ADD COLUMN IF NOT EXISTS {col} {typ}")

    # hash группы: координаты (если есть) надёжнее адреса;
    # sentinel-значения для NULL предотвращают ложные коллизии
    log.info("dedup: computing group hash")
    cur.execute(f"""
        UPDATE {TBL} SET dedup_group_id = hashtext(
            CASE
                WHEN lat IS NOT NULL THEN
                    ROUND(lat::numeric, 4)::text || '|' ||
                    ROUND(lon::numeric, 4)::text || '|' ||
                    coalesce(total_area::text, '') || '|' ||
                    coalesce(floor::text, '') || '|' ||
                    coalesce(rooms::text, '')
                ELSE
                    coalesce(street, '_NO_STREET_') || '|' ||
                    coalesce(house, '_NO_HOUSE_') || '|' ||
                    coalesce(total_area::text, '') || '|' ||
                    coalesce(floor::text, '') || '|' ||
                    coalesce(rooms::text, '')
            END
        )
    """)
    cur.connection.commit()

    # статистика группы
    log.info("dedup: computing group stats")
    cur.execute(f"""
        UPDATE {TBL} t SET
            group_size = sub.cnt,
            group_min_price = sub.min_p,
            group_max_price = sub.max_p,
            group_seller_types = sub.sellers
        FROM (
            SELECT dedup_group_id,
                count(*) as cnt,
                min(price) as min_p,
                max(price) as max_p,
                string_agg(DISTINCT seller_type, ',') as sellers
            FROM {TBL}
            GROUP BY dedup_group_id
        ) sub
        WHERE t.dedup_group_id = sub.dedup_group_id
    """)
    cur.connection.commit()

    # ранжирование: cian > angultiaev > остальные, новее лучше
    log.info("dedup: ranking within groups")
    cur.execute(f"UPDATE {TBL} SET is_primary = FALSE")
    cur.execute(f"""
        UPDATE {TBL} t SET is_primary = TRUE
        FROM (
            SELECT ctid FROM (
                SELECT ctid, row_number() OVER (
                    PARTITION BY dedup_group_id
                    ORDER BY
                        CASE source
                            WHEN 'cian' THEN 0
                            WHEN 'kaggle_angultiaev' THEN 1
                            WHEN 'kaggle_ivan314sh' THEN 2
                            ELSE 3
                        END,
                        pub_date DESC NULLS LAST
                ) as rn
                FROM {TBL}
            ) ranked WHERE rn = 1
        ) sub
        WHERE t.ctid = sub.ctid
    """)
    cur.connection.commit()

    cur.execute(f"SELECT count(*) FROM {TBL} WHERE group_size > 1")
    dup_rows = cur.fetchone()[0]
    cur.execute(f"SELECT count(DISTINCT dedup_group_id) FROM {TBL} WHERE group_size > 1")
    dup_groups = cur.fetchone()[0]
    log.info(f"dedup: {dup_groups} groups, {dup_rows} duplicate rows")
