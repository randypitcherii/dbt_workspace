{{
    config(
        materialized='incremental',
        incremental_key='processed_at'
    )
}}

select * from {{ ref('incremental_source') }}