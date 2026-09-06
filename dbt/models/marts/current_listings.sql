{{ config(pre_hook=["set work_mem = '64MB'", "set max_parallel_workers_per_gather = 0"]) }}

with sane as (
    select *, price / nullif(total_area, 0) as ppm2
    from {{ ref('ml_listings_wide') }}
    where event_closed = 0
      and total_area between 10 and {{ var('max_flat_area') }}
      and price between 300000 and 20000000000
      and region is not null
),
band as (
    select region, coalesce(is_new_building, false) as nb,
           percentile_cont(0.5) within group (order by ppm2) as med,
           count(*) as n
    from sane
    group by 1, 2
)

select
    s.cian_id,
    s.unit_sk,
    s.region,
    s.municipality,
    s.rooms,
    s.flat_type,
    s.total_area,
    s.living_area,
    s.kitchen_area,
    s.price,
    round(ppm2)::bigint as price_per_m2,
    s.is_new_building,
    s.is_studio,
    s.mortgage_allowed,
    s.nearest_metro,
    s.nearest_metro_time,
    s.nearest_metro_walk,
    s.n_metro,
    s.floor,
    s.total_floors,
    s.ceiling_height,
    s.year_built,
    s.renovation,
    s.residential_complex,
    s.developer,
    s.building_type,
    s.parking,
    s.seller_user_type,
    s.seller_is_owner,
    s.is_penthouse,
    s.is_apartments,
    s.room_type,
    s.photos_count,
    s.lat,
    s.lon,
    s.publication_date,
    s.days_on_market
from sane s
join band b on b.region = s.region and b.nb = coalesce(s.is_new_building, false)
where b.n < {{ var('outlier_min_cell') }}
   or s.ppm2 between b.med / {{ var('outlier_low') }} and b.med * {{ var('outlier_high') }}
