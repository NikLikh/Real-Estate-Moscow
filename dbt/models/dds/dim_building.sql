with src as (
    select
        street,
        house,
        lat,
        lon,
        max(year_built) as year_built,
        max(building_type) as building_type,
        bool_or(is_apartments) as is_apartments,
        max(passenger_lifts) as passenger_lifts,
        max(cargo_lifts) as cargo_lifts,
        bool_or(demolished_in_renovation) as demolished_in_renovation
    from {{ ref('stg_cian_observations') }}
    group by street, house, lat, lon
)
select
    {{ surrogate_key(['street', 'house', 'lat', 'lon']) }} as building_sk,
    street,
    house,
    lat,
    lon,
    year_built,
    building_type,
    is_apartments,
    passenger_lifts,
    cargo_lifts,
    demolished_in_renovation
from src
