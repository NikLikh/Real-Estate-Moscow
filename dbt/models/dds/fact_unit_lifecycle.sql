{{ config(pre_hook=["set work_mem = '64MB'", "set max_parallel_workers_per_gather = 0"]) }}

with agg as (
    select
        unit_sk,
        count(*) as n_listings,
        count(distinct cian_user_id) as n_sellers,
        min(first_seen) as first_seen,
        max(last_seen) as last_seen,
        min(coalesce(publication_date, first_seen)) as listed_at,
        bool_or(event_closed = 0) as is_live,
        (array_agg(price_first order by first_seen, cian_id))[1] as price_first,
        (array_agg(price_last order by last_seen desc, cian_id desc))[1] as price_last,
        min(price_min) as price_min,
        max(price_max) as price_max,
        sum(price_points) as price_points
    from {{ ref('fact_listing_lifecycle') }}
    group by unit_sk
),
head as (
    select distinct on (unit_sk)
        unit_sk,
        cian_id,
        geo_sk,
        building_sk,
        price,
        mortgage_allowed,
        deal_conditions,
        n_metro,
        nearest_metro,
        nearest_metro_time,
        nearest_metro_walk,
        rooms,
        is_studio,
        flat_type,
        total_area,
        living_area,
        kitchen_area,
        floor,
        total_floors,
        ceiling_height,
        renovation,
        bathrooms,
        balcony,
        window_view,
        parking,
        is_new_building,
        developer,
        residential_complex,
        completion_date,
        seller_type,
        seller_user_type,
        phone_protected,
        seller_is_owner,
        cian_user_id,
        is_penthouse,
        room_type,
        photos_count
    from {{ ref('fact_listing_lifecycle') }}
    order by unit_sk, (event_closed = 0) desc, last_seen desc, cian_id desc
)
select
    h.*,
    a.n_listings,
    a.n_sellers,
    a.first_seen,
    a.last_seen,
    a.listed_at,
    (a.last_seen::date - a.listed_at::date) as exposure_days,
    (not a.is_live)::int as unit_closed,
    a.price_first,
    a.price_last,
    a.price_min,
    a.price_max,
    a.price_points
from head h
join agg a using (unit_sk)
