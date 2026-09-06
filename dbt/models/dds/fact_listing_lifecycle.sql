{{ config(pre_hook=["set work_mem = '64MB'", "set max_parallel_workers_per_gather = 0"]) }}

with latest as (
    select distinct on (cian_id)
        cian_id,
        price, mortgage_allowed, deal_conditions,
        rooms, is_studio, flat_type, total_area, living_area, kitchen_area,
        floor, total_floors, ceiling_height, renovation,
        bathrooms, balcony, window_view, parking,
        is_new_building, developer, residential_complex, completion_date,
        publication_date, seller_type, seller_user_type, phone_protected,
        photos_count, views_total, views_today,
        seller_is_owner, status, cian_user_id, is_penthouse, room_type
    from {{ ref('stg_cian_observations') }}
    order by cian_id, (region is not null) desc, scraped_at desc
),
geo as (
    select distinct on (cian_id)
        cian_id,
        region, municipality, district, microdistrict,
        street, house, lat, lon,
        metro_stations
    from {{ ref('stg_cian_observations') }}
    where region is not null
    order by cian_id, (municipality is not null) desc, (street is not null) desc, scraped_at desc
),
agg as (
    select
        cian_id,
        min(scraped_at) as first_seen,
        max(scraped_at) as last_seen,
        (array_agg(price order by scraped_at, obs_id))[1] as price_first,
        (array_agg(price order by scraped_at desc, obs_id desc))[1] as price_last,
        min(price) as price_min,
        max(price) as price_max,
        count(*) as price_points
    from {{ ref('stg_cian_observations') }}
    group by cian_id
),
unit_span as (
    select
        l.cian_id,
        {{ surrogate_key(['round(g.lat::numeric, 5)', 'round(g.lon::numeric, 5)', 'l.floor', 'round(l.total_area::numeric, 1)']) }} as class_sk,
        coalesce(l.cian_user_id, -l.cian_id) as seller,
        a.first_seen,
        a.last_seen,
        coalesce(a.price_last, 0) as price
    from latest l
    join agg a using (cian_id)
    join geo g using (cian_id)
    where g.lat is not null and l.total_area is not null
),
seller_price as (
    select class_sk, seller,
           percentile_cont(0.5) within group (order by price) as price
    from unit_span
    group by class_sk, seller
),
seller_break as (
    select class_sk, seller, price,
        case
            when lag(price) over (partition by class_sk order by price, seller)
                 >= price * (1 - {{ var('unit_price_tolerance') }})
            then 0 else 1
        end as brk
    from seller_price
),
seller_group as (
    select class_sk, seller,
        sum(brk) over (partition by class_sk order by price, seller rows unbounded preceding) as price_grp
    from seller_break
),
unit_class as (
    select
        u.cian_id,
        u.seller,
        u.first_seen,
        u.last_seen,
        {{ surrogate_key(['u.class_sk', 'g.price_grp']) }} as class_sk
    from unit_span u
    join seller_group g using (class_sk, seller)
),
unit_event as (
    select class_sk, seller, cian_id, first_seen as ts, 1 as delta from unit_class
    union all
    select class_sk, seller, cian_id, last_seen as ts, -1 as delta from unit_class
),
unit_slot as (
    select cian_id, class_sk, concurrent - 1 as slot
    from (
        select
            class_sk, cian_id, delta,
            sum(delta) over (
                partition by class_sk, seller
                order by ts, delta desc, cian_id
                rows unbounded preceding
            ) as concurrent
        from unit_event
    ) s
    where delta = 1
),
seen as (
    select distinct cian_id, scraped_at::date as d
    from {{ ref('stg_cian_observations') }}
    where scraped_at >= current_date - {{ var('census_window_days') }}
),
region_days as (
    select g.region, s.d, count(*) as n
    from seen s
    join geo g using (cian_id)
    group by 1, 2
),
census as (
    select region, d, row_number() over (partition by region order by d desc) as rn
    from (
        select region, d, n, max(n) over (partition by region) as mx
        from region_days
    ) x
    where n >= {{ var('census_floor') }} * mx
),
cutoff as (
    select region, min(d) as last_day
    from census
    where rn <= {{ var('closure_missed_days') }}
    group by region
)
select
    l.cian_id,
    {{ surrogate_key(['coalesce(u.class_sk, l.cian_id::text)', 'u.slot']) }} as unit_sk,
    u.slot as unit_slot,
    {{ surrogate_key(['g.region', 'g.municipality', 'g.district', 'g.microdistrict']) }} as geo_sk,
    {{ surrogate_key(['g.street', 'g.house', 'g.lat', 'g.lon']) }} as building_sk,
    l.price,
    l.mortgage_allowed,
    l.deal_conditions,
    coalesce(jsonb_array_length(g.metro_stations), 0) as n_metro,
    g.metro_stations -> 0 ->> 0 as nearest_metro,
    (g.metro_stations -> 0 ->> 1)::int as nearest_metro_time,
    (g.metro_stations -> 0 ->> 2 = 'walk') as nearest_metro_walk,
    l.rooms,
    l.is_studio,
    l.flat_type,
    l.total_area,
    l.living_area,
    l.kitchen_area,
    l.floor,
    l.total_floors,
    l.ceiling_height,
    l.renovation,
    l.bathrooms,
    l.balcony,
    l.window_view,
    l.parking,
    l.is_new_building,
    l.developer,
    l.residential_complex,
    l.completion_date,
    l.seller_type,
    l.seller_user_type,
    l.phone_protected,
    l.photos_count,
    l.views_total,
    l.views_today,
    l.seller_is_owner,
    l.status,
    l.cian_user_id,
    l.is_penthouse,
    l.room_type,
    a.first_seen,
    a.last_seen,
    l.publication_date,
    (a.last_seen::date - coalesce(l.publication_date::date, a.first_seen::date)) as days_on_market,
    coalesce((a.last_seen::date < c.last_day)::int, 0) as event_closed,
    a.price_first,
    a.price_last,
    a.price_min,
    a.price_max,
    a.price_points
from latest l
join agg a using (cian_id)
left join geo g using (cian_id)
left join unit_slot u using (cian_id)
left join cutoff c on c.region = g.region
