with live as (
    select count(*) as n, count(cian_user_id) as with_seller
    from {{ ref('fact_listing_lifecycle') }}
    where event_closed = 0
)
select n, with_seller
from live
where with_seller < n * 0.95
