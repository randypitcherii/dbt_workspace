-- Downloaded this seed set from https://www.kaggle.com/romanzdk/zodiacs-with-corresponding-dates

{{
    config(
        materialized='table'
    )
}}

{% set min_date = '(select min(date_start) from ' ~ source('zodiac', 'zodiac') ~ ')' %}
{% set max_date = '(select max(date_end)   from ' ~ source('zodiac', 'zodiac') ~ ')' %}

with

dim_days as (
    {{ dbt_utils.date_spine(
        datepart='day',
        start_date= min_date,
        end_date= max_date
    )}}
)

select
    DATE_DAY AS DAY,
    ZODIAC AS ZODIAC_SIGN

from 
    dim_days join {{ source('zodiac', 'zodiac') }}
    on date_day between date_start and date_end

union all

select '2022-03-11'::date as DAY, null as zodiac_sign