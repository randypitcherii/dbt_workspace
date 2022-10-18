{{config(materialized='incremental')}}

select * from {{ref('raw_netflix_data')}} limit 10