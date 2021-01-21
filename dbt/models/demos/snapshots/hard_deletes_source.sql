{{ config(materialized='table') }}

select
    1 as id,
    'blue' as color

union all

select
    2 as id,
    'yellow' as color

union all

select
    3 as id,
    'red' as color

union all

select
    4 as id,
    'green' as color