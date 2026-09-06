with cur as (
    select
        coalesce(deal_type, 'sale') as deal_type,
        region,
        coalesce(is_new_building, false) as nb,
        price / total_area as ppm2
    from {{ source('raw', 'cian_observations') }}
    where scraped_at >= now() - interval '2 days'
      and url is not null
      and region is not null
      and price > 0
      and total_area between 8 and {{ var('max_flat_area') }}
      and coalesce(deal_type, 'sale') <> 'rent_day'
),
band as (
    select deal_type, region, nb,
           percentile_cont(0.5) within group (order by ppm2) as med,
           count(*) as n
    from cur
    group by 1, 2, 3
    having count(*) >= {{ var('outlier_min_cell') }}
),
scored as (
    select c.deal_type,
           count(*) as n,
           count(*) filter (
               where c.ppm2 < b.med / {{ var('outlier_low') }}
                  or c.ppm2 > b.med * {{ var('outlier_high') }}
           ) as bad
    from cur c
    join band b using (deal_type, region, nb)
    group by 1
)
select deal_type, n, bad, round(100.0 * bad / n, 3) as pct
from scored
where n > 10000 and bad > n * 0.002
