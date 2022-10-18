{{
    config(
        materialized='table'
    )
}}

select 
    current_timestamp as processed_at,
    zodiac as zodiac_sign,
    'blue' as color
    
from {{ ref('zodiac') }}