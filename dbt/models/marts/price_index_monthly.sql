{{ config(pre_hook=["set work_mem = '64MB'", "set max_parallel_workers_per_gather = 0"]) }}

with static as (
    select
        l.cian_id,
        l.unit_sk,
        u.total_area,
        u.is_new_building,
        g.region,
        g.municipality
    from {{ ref('fact_listing_lifecycle') }} l
    join {{ ref('fact_unit_lifecycle') }} u using (unit_sk)
    join {{ ref('dim_geo') }} g on g.geo_sk = u.geo_sk
    where u.total_area between 10 and 400 and g.municipality is not null
),
obs as (
    select
        cian_id,
        date_trunc('month', scraped_at)::date as month,
        percentile_cont(0.5) within group (order by price) as price
    from {{ ref('stg_cian_observations') }}
    where price between 300000 and 2000000000
    group by 1, 2
),
units as (
    select
        s.unit_sk,
        o.month,
        s.region,
        s.municipality,
        s.is_new_building,
        percentile_cont(0.5) within group (order by o.price / s.total_area) as ppm2
    from obs o
    join static s using (cian_id)
    group by 1, 2, 3, 4, 5
)
select
    month,
    region,
    municipality,
    is_new_building,
    round(percentile_cont(0.5) within group (order by ppm2))::numeric as median_ppm2,
    count(*) as n_points
from units
group by month, region, municipality, is_new_building
having count(*) >= 20
