{{
    config(
        materialized='incremental',
        unique_key="zodiac_sign || ' - ' || color",
        incremental_key='processed_at'
    )
}}



select * from {{ ref('incremental_source') }}