{{
    config(
        materialized='incremental',
        incremental_key='processed_at'
    )
}}

select * from {{ ref('a__incremental_source') }}