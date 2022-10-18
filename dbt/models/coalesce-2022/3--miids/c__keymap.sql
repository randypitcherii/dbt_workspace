select 
    natural_key,
    miid,
    processed_at

from {{ ref('a__miid') }}