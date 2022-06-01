{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    
    {% set log_msg='getting custom schema:\ntarget_name:' ~ target.name ~ '\ncustom_schema_name:' ~ custom_schema_name %}
    {% do log(log_msg, False) %}

    {%- if custom_schema_name -%}
        {{ custom_schema_name if 'prod' in target.name.lower() else target.schema ~ '__' ~ custom_schema_name }}
    {%- else -%}
        {{ default_schema }} 
    {%- endif -%}

{%- endmacro %}