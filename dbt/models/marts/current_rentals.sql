{{ config(pre_hook=["set work_mem = '64MB'", "set max_parallel_workers_per_gather = 0"]) }}

with sane as (
    select *, price / nullif(total_area, 0) as ppm2
    from {{ ref('fact_rent_lifecycle') }}
    where event_closed = 0
      and region is not null
      and total_area between 5 and {{ var('max_flat_area') }}
      and (
            (deal_type = 'rent_long' and price between 3000 and 3000000)
         or (deal_type = 'rent_day' and price between 300 and 500000)
      )
),
band as (
    select region, deal_type,
           percentile_cont(0.5) within group (order by ppm2) as med,
           count(*) as n
    from sane
    group by 1, 2
)

select
    cian_id,
    deal_type,
    region,
    municipality,
    district,
    street,
    house,
    lat,
    lon,
    nearest_metro,
    nearest_metro_time,
    nearest_metro_walk,
    n_metro,
    rooms,
    is_studio,
    flat_type,
    total_area,
    living_area,
    kitchen_area,
    beds_count,
    floor,
    total_floors,
    ceiling_height,
    year_built,
    building_type,
    is_apartments,
    is_new_building,
    residential_complex,
    renovation,
    parking,
    price,
    round(ppm2)::bigint as price_per_m2,
    deposit,
    agent_fee,
    client_fee,
    prepay_months,
    lease_term_type,
    payment_period,
    utilities_included,
    utilities_price,
    pets_allowed,
    children_allowed,
    has_furniture,
    has_fridge,
    has_washer,
    has_dishwasher,
    has_conditioner,
    has_tv,
    has_internet,
    seller_type,
    seller_user_type,
    seller_is_owner,
    is_agent,
    agency_name,
    photos_count,
    publication_date,
    first_seen,
    last_seen,
    days_on_market
from sane s
join band b using (region, deal_type)
where s.deal_type = 'rent_day'
   or b.n < {{ var('outlier_min_cell') }}
   or s.ppm2 between b.med / {{ var('outlier_low') }} and b.med * {{ var('outlier_high') }}
