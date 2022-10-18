

select * from {{ ref('b__miid') }}




















{{
    config(
        materialized='ephemeral'
    )
}}