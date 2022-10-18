select 
    natural_key,
    miid

from {{ ref('a__miid') }}