{{ config(pre_hook=["set work_mem = '64MB'", "set max_parallel_workers_per_gather = 0"]) }}

with static as (
    select cian_id, total_area, is_new_building, region, municipality
    from {{ ref('ml_listings_wide') }}
    where total_area between 10 and 400 and municipality is not null
),
obs as (
    select cian_id, date_trunc('month', scraped_at)::date as month, price
    from {{ ref('stg_cian_observations') }}
    where price between 300000 and 2000000000
)
select
    o.month,
    s.region,
    s.municipality,
    s.is_new_building,
    round(percentile_cont(0.5) within group (order by o.price / s.total_area))::numeric as median_ppm2,
    count(*) as n_points
from obs o
join static s using (cian_id)
group by o.month, s.region, s.municipality, s.is_new_building
having count(*) >= 20
