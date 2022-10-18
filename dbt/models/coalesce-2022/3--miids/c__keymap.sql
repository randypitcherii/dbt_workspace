{{
    config(
        materialized='incremental',
        unique_key='natural_key',
        incremental_key='processed_at'
    )
}}

select 
    natural_key,
    miid,
    processed_at

from {{ ref('a__miid') }}