select max(scraped_at) as last_scraped_at
from {{ source('raw', 'cian_observations') }}
having max(scraped_at) < now() - interval '14 hours'
