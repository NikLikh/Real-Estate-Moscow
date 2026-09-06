with bounds as (
    select (now() at time zone 'Europe/Moscow')::date as today
),
daily as (
    select (scraped_at at time zone 'Europe/Moscow')::date as d,
           count(distinct cian_id) as n
    from {{ source('raw', 'cian_observations') }}
    where scraped_at >= now() - interval '16 days'
    group by 1
),
expected as (
    select max(n) as peak
    from daily, bounds
    where daily.d < bounds.today
),
yesterday as (
    select coalesce((select n from daily, bounds where daily.d = bounds.today - 1), 0) as n
)
select (select today - 1 from bounds) as d, yesterday.n, expected.peak
from expected, yesterday
where yesterday.n < expected.peak * 0.6
