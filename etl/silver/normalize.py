import logging

log = logging.getLogger("re")

# маппинг сокращений building_type к полным названиям
BUILDING_TYPE_NORM = {
    # русские сокращения
    "Монолит": "Монолитный",
    "Панель": "Панельный",
    "Кирпич": "Кирпичный",
    "Блоки": "Блочный",
    "Дерево": "Деревянный",
    # варианты русских названий
    "Монолит-кирпич": "Монолитно-кирпичный",
    # англ. значения из CIAN JSON API (building.materialType)
    "monolith": "Монолитный",
    "panel": "Панельный",
    "brick": "Кирпичный",
    "monolithBrick": "Монолитно-кирпичный",
    "block": "Блочный",
    "wood": "Деревянный",
    "stalin": "Сталинский",
    "boards": "Щитовой",
}

RENOVATION_NORM = {
    # англ. из CIAN JSON (repairType) -- capitalized
    "Cosmetic": "Косметический",
    "European-style renovation": "Евроремонт",
    "Without renovation": "Без ремонта",
    "Designer renovation": "Дизайнерский",
    "Designer": "Дизайнерский",
    # англ. из CIAN JSON -- lowercase
    "cosmetic": "Косметический",
    "euro": "Евроремонт",
    "no": "Без ремонта",
    "designer": "Дизайнерский",
    # русские варианты из Kaggle/HTML
    "Предчистовая": "Предчистовая отделка",
    "Без отделки": "Без отделки",
    "Муниципальный ремонт": "Косметический",
}

SELLER_TYPE_NORM = {
    "agency": "Агентство",
    "homeowner": "Собственник",
    "developer": "Застройщик",
}


def _build_case(col_name, mapping, null_values=None, else_null=False):
    """CASE col WHEN ... из словаря маппинга"""
    parts = [f"WHEN '{old}' THEN '{new}'" for old, new in mapping.items()]
    if null_values:
        for v in null_values:
            parts.append(f"WHEN '{v}' THEN NULL")
    else_clause = "ELSE NULL" if else_null else f"ELSE {col_name}"
    return f"CASE {col_name} " + " ".join(parts) + f" {else_clause} END"


def normalize(cur):
    """нормализация всех категориальных полей"""

    # region: приводим к двум значениям
    cur.execute("""
        UPDATE silver_staging SET region = CASE
            WHEN region ILIKE '%московск%област%'
              OR region ILIKE '%подмосков%'
              THEN 'Московская область'
            WHEN region ILIKE '%москва%'
              THEN 'Москва'
            ELSE region
        END
        WHERE region IS NOT NULL
    """)
    log.info(f"normalize region text: {cur.rowcount}")

    # fallback по координатам для оставшихся (bbox Москвы)
    cur.execute("""
        UPDATE silver_staging SET region =
            CASE
                WHEN lat BETWEEN 55.48 AND 55.95
                 AND lon BETWEEN 37.32 AND 37.88
                THEN 'Москва'
                ELSE 'Московская область'
            END
        WHERE lat IS NOT NULL
          AND (region IS NULL OR region NOT IN ('Москва', 'Московская область'))
    """)
    log.info(f"normalize region by coords: {cur.rowcount}")

    # студии: cian пишет rooms=-1, унифицируем в 0
    cur.execute("""
        UPDATE silver_staging SET rooms = 0 WHERE rooms = -1
    """)
    log.info(f"normalize rooms (-1 to 0): {cur.rowcount}")

    # building_type
    bt_case = _build_case(
        "building_type", BUILDING_TYPE_NORM,
        null_values=["Другое"], else_null=True,
    )
    cur.execute(f"""
        UPDATE silver_staging SET building_type = {bt_case}
        WHERE building_type IS NOT NULL
    """)
    log.info(f"normalize building_type: {cur.rowcount}")

    # renovation
    ren_case = _build_case("renovation", RENOVATION_NORM)
    cur.execute(f"""
        UPDATE silver_staging SET renovation = {ren_case}
        WHERE renovation IN ({','.join(f"'{k}'" for k in RENOVATION_NORM)})
    """)
    log.info(f"normalize renovation: {cur.rowcount}")

    # seller_type
    st_case = _build_case("seller_type", SELLER_TYPE_NORM)
    cur.execute(f"""
        UPDATE silver_staging SET seller_type = {st_case}
        WHERE seller_type IN ({','.join(f"'{k}'" for k in SELLER_TYPE_NORM)})
    """)
    log.info(f"normalize seller_type: {cur.rowcount}")

    # is_new_building: NULL считаем за вторичку
    cur.execute("""
        UPDATE silver_staging SET is_new_building = FALSE
        WHERE is_new_building IS NULL
    """)
    log.info(f"normalize is_new_building NULL to FALSE: {cur.rowcount}")

    # is_apartments: аналогично
    cur.execute("""
        UPDATE silver_staging SET is_apartments = FALSE
        WHERE is_apartments IS NULL
    """)
    log.info(f"normalize is_apartments NULL to FALSE: {cur.rowcount}")

    # street: раскрываем сокращения (ул., просп., пер. и т.д.)
    cur.execute(r"""
        UPDATE silver_staging SET street = TRIM(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                      regexp_replace(
                        regexp_replace(street,
                          '\mул\.\s*', 'улица ', 'i'),
                        '\mпросп\.\s*', 'проспект ', 'i'),
                      '\mпр-т\s+', 'проспект ', 'i'),
                    '\mпер\.\s*', 'переулок ', 'i'),
                  '\mнаб\.\s*', 'набережная ', 'i'),
                '\mбульв\.\s*', 'бульвар ', 'i'),
              '\mш\.\s*', 'шоссе ', 'i')
        )
        WHERE street IS NOT NULL
    """)
    log.info(f"normalize street: {cur.rowcount}")

    # house: убираем "д.", "корп." заменяем на "к", склеиваем пробелы
    cur.execute(r"""
        UPDATE silver_staging SET house = TRIM(
            regexp_replace(
              regexp_replace(
                regexp_replace(house,
                  '^\s*д\.?\s*', '', 'i'),
                '\s*корп\.?\s*', 'к', 'i'),
              '\s+', '', 'g')
        )
        WHERE house IS NOT NULL
    """)
    log.info(f"normalize house: {cur.rowcount}")

    # building_era по году постройки
    cur.execute("""
        ALTER TABLE silver_staging ADD COLUMN IF NOT EXISTS building_era text
    """)
    cur.execute("""
        UPDATE silver_staging SET building_era =
            CASE
                WHEN year_built < 1941 THEN 'Довоенный'
                WHEN year_built < 1957 THEN 'Сталинка'
                WHEN year_built < 1972 THEN 'Хрущёвка'
                WHEN year_built < 1986 THEN 'Брежневка'
                WHEN year_built < 2000 THEN 'Современный'
                ELSE 'Новый'
            END
        WHERE year_built IS NOT NULL
    """)
    log.info(f"normalize building_era: {cur.rowcount}")

    # crossfill building_type по координатам (один дом = одинаковый тип)
    cur.execute("""
        UPDATE silver_staging t SET building_type = sub.bt
        FROM (
            SELECT ROUND(lat::numeric, 4) as lat4, ROUND(lon::numeric, 4) as lon4,
                   MODE() WITHIN GROUP (ORDER BY building_type) as bt
            FROM silver_staging
            WHERE building_type IS NOT NULL AND lat IS NOT NULL
            GROUP BY lat4, lon4
        ) sub
        WHERE t.building_type IS NULL AND t.lat IS NOT NULL
          AND ROUND(t.lat::numeric, 4) = sub.lat4
          AND ROUND(t.lon::numeric, 4) = sub.lon4
    """)
    log.info(f"crossfill building_type by coords: {cur.rowcount}")

    cur.connection.commit()
