{{ config(materialized='table') }}

-- ORDER_ID 	COLOR	ORDER_DATE	STATUS	LOADED_AT

select
    1 as order_id,
    'RED' as color,
    current_timestamp()::date - 1 as ORDER_DATE,
    'ORDERED' as status,
    current_timestamp() as LOADED_AT

union all

select
    2 as order_id,
    'BLUE' as color,
    current_timestamp()::date - 1 as ORDER_DATE,
    'ORDERED' as status,
    current_timestamp() as LOADED_AT

union all

select
    3 as order_id,
    'GREEN' as color,
    current_timestamp()::date - 1 as ORDER_DATE,
    'ORDERED' as status,
    current_timestamp() as LOADED_AT

union all

select
    4 as order_id,
    'PURPLE' as color,
    current_timestamp()::date - 1 as ORDER_DATE,
    'ORDERED' as status,
    current_timestamp() as LOADED_AT

