select distinct
    {{ surrogate_key(['region', 'municipality', 'district', 'microdistrict']) }} as geo_sk,
    region,
    municipality,
    district,
    microdistrict
from {{ ref('stg_cian_observations') }}
