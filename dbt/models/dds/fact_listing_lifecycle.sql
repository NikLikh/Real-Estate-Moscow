with latest as (
    select distinct on (cian_id)
        cian_id,
        region, municipality, district, microdistrict, street, house, lat, lon,
        price, mortgage_allowed, deal_conditions,
        metro_stations,
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
bounds as (
    select max(d) as last_day
    from (
        select scraped_at::date as d, count(distinct cian_id) as n
        from {{ ref('stg_cian_observations') }}
        group by scraped_at::date
    ) daily
    where n >= 100000
)
select
    l.cian_id,
    {{ surrogate_key(['l.region', 'l.municipality', 'l.district', 'l.microdistrict']) }} as geo_sk,
    {{ surrogate_key(['l.street', 'l.house', 'l.lat', 'l.lon']) }} as building_sk,
    l.price,
    l.mortgage_allowed,
    l.deal_conditions,
    coalesce(jsonb_array_length(l.metro_stations), 0) as n_metro,
    l.metro_stations -> 0 ->> 0 as nearest_metro,
    (l.metro_stations -> 0 ->> 1)::int as nearest_metro_time,
    (l.metro_stations -> 0 ->> 2 = 'walk') as nearest_metro_walk,
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
    (a.last_seen::date < (select last_day from bounds))::int as event_closed,
    a.price_first,
    a.price_last,
    a.price_min,
    a.price_max,
    a.price_points
from latest l
join agg a using (cian_id)
