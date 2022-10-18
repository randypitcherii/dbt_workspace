select 
    zodiac_sign || ' - ' || color as natural_key,
    miid

from {{ ref('a__miid_raw_sql') }}