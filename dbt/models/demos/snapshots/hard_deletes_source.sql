{{ config(materialized='table') }}

select
    1 as id,
    'blue' as color,
    42 as the_answer,
    current_timestamp() as insert_time

union all

select
    2 as id,
    'yellow' as color,
    42 as the_answer,
    current_timestamp() as insert_time

union all

select
    3 as id,
    'maroon' as color,
    42 as the_answer,
    current_timestamp() as insert_time

union all

select
    4 as id,
    'green' as color,
    42 as the_answer,
    current_timestamp() as insert_time

union all

select
    100 as id,
    'black' as color,
    42 as the_answer,
    current_timestamp() as insert_time