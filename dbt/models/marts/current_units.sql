{{ config(pre_hook=["set work_mem = '64MB'", "set max_parallel_workers_per_gather = 0"]) }}

select
    u.unit_sk,
    u.cian_id,
    g.region,
    g.municipality,
    u.rooms,
    u.flat_type,
    u.total_area,
    u.living_area,
    u.kitchen_area,
    u.price,
    round(u.price / nullif(u.total_area, 0))::bigint as price_per_m2,
    u.is_new_building,
    u.is_studio,
    u.mortgage_allowed,
    u.nearest_metro,
    u.nearest_metro_time,
    u.nearest_metro_walk,
    u.n_metro,
    u.floor,
    u.total_floors,
    u.ceiling_height,
    b.year_built,
    u.renovation,
    u.residential_complex,
    u.developer,
    b.building_type,
    u.parking,
    u.seller_user_type,
    u.seller_is_owner,
    u.is_penthouse,
    b.is_apartments,
    u.room_type,
    u.photos_count,
    b.lat,
    b.lon,
    u.listed_at,
    u.exposure_days,
    u.n_listings,
    u.n_sellers
from {{ ref('fact_unit_lifecycle') }} u
left join {{ ref('dim_geo') }} g on g.geo_sk = u.geo_sk
left join {{ ref('dim_building') }} b on b.building_sk = u.building_sk
where u.unit_closed = 0
  and u.total_area between 10 and 400
  and u.price between 300000 and 2000000000
