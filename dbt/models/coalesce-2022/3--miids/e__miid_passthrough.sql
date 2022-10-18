
select 
    natural_key, 
    zodiac_sign, 
    color,
    {{coalesce_prettify_timestamp('processed_at')}} as processed_at,

from {{ ref('a__miid') }}

order by miid asc




















{{
    config(
        materialized='ephemeral'
    )
}}