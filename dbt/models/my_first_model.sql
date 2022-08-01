{{config(materialized='table')}}

select 'hello, world!' as col, current_timestamp as now