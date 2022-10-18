{{
    config(
        materialized='raw_sql',
    )
}}

{% set name %}
    {{this.database}}.{{this.schema}}.{{this.identifier}}
{% endset %}

create or replace table 
{{name}} (
    id number autoincrement start 1 increment 1,
    zodiac_sign string,
    color string 
);