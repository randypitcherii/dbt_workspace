{{
    config(
        materialized='raw_sql',
    )
}}

{% set name %}
    {{this.database}}.{{this.schema}}.{{this.identifier}}
{% endset %}

-- create miids incrementally for {{ ref('a__incremental_source') }}

create table if not exists  
{{name}} (
    miid         number autoincrement start 1 increment 1,
    natural_key  string,
    zodiac_sign  string,
    color        string,
    processed_at timestamp_tz
);

