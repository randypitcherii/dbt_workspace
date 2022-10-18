

select 
    {{coalesce_prettify_timestamp('processed_at')}} as processed_at,
    zodiac_sign,
    color

from {{ ref('zodiac_incremental') }}

order by processed_at desc, zodiac_sign asc














{{
    config(
        materialized='ephemeral'
    )
}}