with recent as (
    select deal_type, price
    from {{ ref('stg_cian_rent') }}
    where scraped_at >= now() - interval '2 days'
),
flagged as (
    select
        count(*) as n,
        count(*) filter (
            where (deal_type = 'rent_long' and (price < 3000 or price > 3000000))
               or (deal_type = 'rent_day' and (price < 300 or price > 500000))
        ) as bad
    from recent
)
select n, bad, round(100.0 * bad / nullif(n, 0), 2) as pct
from flagged
where n > 1000 and bad > n * 0.02
