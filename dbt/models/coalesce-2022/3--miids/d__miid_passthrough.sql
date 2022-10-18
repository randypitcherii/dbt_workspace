

select * from {{ ref('a__miid') }}




















{{
    config(
        materialized='ephemeral'
    )
}}