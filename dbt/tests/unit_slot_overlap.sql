with l as (
    select unit_sk, cian_id, cian_user_id, first_seen, last_seen
    from {{ ref('fact_listing_lifecycle') }}
    where cian_user_id is not null
),
bad as (
    select count(distinct a.unit_sk) as n
    from l a
    join l b
      on b.unit_sk = a.unit_sk
     and b.cian_user_id = a.cian_user_id
     and b.cian_id > a.cian_id
     and b.first_seen <= a.last_seen
     and a.first_seen <= b.last_seen
),
total as (
    select count(*) as n from {{ ref('fact_unit_lifecycle') }}
)
select bad.n as overlapping_units, total.n as units
from bad, total
where bad.n > total.n * 0.01
