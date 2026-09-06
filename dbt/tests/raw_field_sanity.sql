with recent as (
    select
        total_area, living_area, kitchen_area, ceiling_height,
        floor, total_floors, year_built, lat, lon, rooms, price
    from {{ source('raw', 'cian_observations') }}
    where scraped_at >= now() - interval '2 days'
      and url is not null
),
flagged as (
    select
        count(*) as n,
        count(*) filter (where total_area is not null and total_area not between 5 and 3000) as bad_area,
        count(*) filter (where living_area > total_area) as bad_living,
        count(*) filter (where kitchen_area > total_area) as bad_kitchen,
        count(*) filter (where ceiling_height is not null and ceiling_height not between 2 and 10) as bad_ceiling,
        count(*) filter (where floor > total_floors) as bad_floor,
        count(*) filter (where year_built is not null and year_built not between 1700 and extract(year from now()) + 8) as bad_year,
        count(*) filter (where lat is not null and (lat not between 41 and 82 or lon not between 19 and 191)) as bad_geo,
        count(*) filter (where rooms is not null and rooms not between 0 and 30) as bad_rooms,
        count(*) filter (where price <= 0) as bad_price
    from recent
)
select *
from flagged
where n > 10000
  and bad_area + bad_living + bad_kitchen + bad_ceiling + bad_floor
    + bad_year + bad_geo + bad_rooms + bad_price > 0
