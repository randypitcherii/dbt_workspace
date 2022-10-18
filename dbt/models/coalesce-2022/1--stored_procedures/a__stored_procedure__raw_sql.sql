{{
    config(
        materialized='raw_sql'
    )
}}

{% set stored_proc_name %}
    {{this.database}}.{{this.schema}}.{{this.identifier}}
{% endset %}
    
create or replace procedure {{stored_proc_name}}()
returns float not null
language javascript
as
$$
    return 3.1415926;
$$
;
    
