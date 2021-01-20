{{ config(materialized='table') }}

select
    1 as id,
    'blue' as color


union all

select
    3 as id,
    'purple' as color

union all

select
    4 as id,
    'green' as color