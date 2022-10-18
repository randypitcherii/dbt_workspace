{{
    config(
        materialized='table'
    )
}}

select 
    current_timestamp as processed_at,
    zodiac as zodiac_sign,
    'purple' as color
    
from {{ ref('zodiac') }}