select
    scraped_at::date as day,
    district,
    count(*) as n_listings,
    avg(price_per_m2) as avg_ppm2,
    avg(price) as avg_price
from {{ ref('stg_cian_observations') }}
group by scraped_at::date, district
