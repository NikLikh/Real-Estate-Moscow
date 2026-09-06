with recent as (
    select sum(cards) as cards, max(plan_offers) as plan_offers, sum(incomplete) as incomplete
    from {{ source('raw', 'scrape_runs') }}
    where finished_at >= now() - interval '30 hours'
      and plan_offers > 0
)
select cards, plan_offers, incomplete,
       round(100.0 * cards / plan_offers, 1) as pct
from recent
where cards < plan_offers * 0.6
