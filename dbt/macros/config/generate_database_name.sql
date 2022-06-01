{% macro generate_database_name(custom_database_name, node) -%}
    {%- set default_database = target.database -%}
    
    {% set log_msg='getting custom database:\ntarget_name:' ~ target.name ~ '\ncustom_database_name:' ~ custom_database_name %}
    {% do log(log_msg, False) %}

    {%- if custom_database_name is none -%}
        {{ default_database }} 
    {%- else -%}
        {{ custom_database_name }}_{{ target.name | trim }}
    {%- endif -%}
{%- endmacro %}