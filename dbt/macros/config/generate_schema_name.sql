{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if 'prod' in target.name.lower() -%}
        {{ custom_schema_name if custom_schema_name else target.schema }}
    {%- else -%}
        {{ target.schema }} 
    {%- endif -%}
{%- endmacro %}