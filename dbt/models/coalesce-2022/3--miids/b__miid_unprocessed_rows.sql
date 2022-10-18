
select 
    zodiac_sign || ' - ' || color as natural_key,
    *

from {{ ref('a__incremental_source') }}

where 
    processed_at > (select coalesce(max(a_miid.processed_at),0::timestamp) from {{ ref('a__miid') }} a_miid) 
