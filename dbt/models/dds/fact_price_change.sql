{{ config(pre_hook=["set work_mem = '64MB'", "set max_parallel_workers_per_gather = 0"]) }}

with seq as (
    select
        cian_id,
        scraped_at as changed_at,
        price,
        lag(price) over (partition by cian_id order by scraped_at) as old_price
    from {{ ref('stg_cian_observations') }}
)
select
    cian_id,
    changed_at,
    old_price,
    price as new_price
from seq
where old_price is not null and old_price <> price
