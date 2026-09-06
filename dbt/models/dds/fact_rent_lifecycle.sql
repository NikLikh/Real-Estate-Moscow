{{ config(pre_hook=["set work_mem = '64MB'", "set max_parallel_workers_per_gather = 0"]) }}

with latest as (
    select distinct on (cian_id)
        cian_id,
        deal_type,
        price, currency,
        deposit, agent_fee, client_fee, prepay_months,
        lease_term_type, payment_period, utilities_included, utilities_price,
        beds_count, pets_allowed, children_allowed,
        has_furniture, has_fridge, has_washer, has_dishwasher,
        has_conditioner, has_tv, has_internet,
        rooms, is_studio, flat_type, total_area, living_area, kitchen_area,
        floor, total_floors, ceiling_height, renovation,
        bathrooms, balcony, window_view, parking,
        is_apartments, year_built, building_type,
        is_new_building, residential_complex,
        publication_date, seller_type, seller_user_type, seller_is_owner,
        phone_protected, photos_count, views_total, views_today,
        status, cian_user_id, is_penthouse, room_type, is_agent, agency_name
    from {{ ref('stg_cian_rent') }}
    order by cian_id, (region is not null) desc, scraped_at desc
),
geo as (
    select distinct on (cian_id)
        cian_id,
        region, municipality, district, microdistrict,
        street, house, lat, lon, metro_stations
    from {{ ref('stg_cian_rent') }}
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
    from {{ ref('stg_cian_rent') }}
    group by cian_id
),
seen as (
    select distinct cian_id, scraped_at::date as d
    from {{ ref('stg_cian_rent') }}
    where scraped_at >= current_date - {{ var('census_window_days') }}
),
region_days as (
    select g.region, l.deal_type, s.d, count(*) as n
    from seen s
    join geo g using (cian_id)
    join latest l using (cian_id)
    group by 1, 2, 3
),
census as (
    select region, deal_type, d, row_number() over (partition by region, deal_type order by d desc) as rn
    from (
        select region, deal_type, d, n, max(n) over (partition by region, deal_type) as mx
        from region_days
    ) x
    where n >= {{ var('census_floor') }} * mx
),
cutoff as (
    select region, deal_type, min(d) as last_day
    from census
    where rn <= {{ var('closure_missed_days') }}
    group by region, deal_type
)
select
    l.cian_id,
    l.deal_type,
    g.region,
    g.municipality,
    g.district,
    g.microdistrict,
    g.street,
    g.house,
    g.lat,
    g.lon,
    coalesce(jsonb_array_length(g.metro_stations), 0) as n_metro,
    g.metro_stations -> 0 ->> 0 as nearest_metro,
    (g.metro_stations -> 0 ->> 1)::int as nearest_metro_time,
    (g.metro_stations -> 0 ->> 2 = 'walk') as nearest_metro_walk,
    l.price,
    l.currency,
    l.deposit,
    l.agent_fee,
    l.client_fee,
    l.prepay_months,
    l.lease_term_type,
    l.payment_period,
    l.utilities_included,
    l.utilities_price,
    l.beds_count,
    l.pets_allowed,
    l.children_allowed,
    l.has_furniture,
    l.has_fridge,
    l.has_washer,
    l.has_dishwasher,
    l.has_conditioner,
    l.has_tv,
    l.has_internet,
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
    l.is_apartments,
    l.year_built,
    l.building_type,
    l.is_new_building,
    l.residential_complex,
    l.seller_type,
    l.seller_user_type,
    l.seller_is_owner,
    l.phone_protected,
    l.photos_count,
    l.views_total,
    l.views_today,
    l.status,
    l.cian_user_id,
    l.is_penthouse,
    l.room_type,
    l.is_agent,
    l.agency_name,
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
left join cutoff c on c.region = g.region and c.deal_type = l.deal_type
