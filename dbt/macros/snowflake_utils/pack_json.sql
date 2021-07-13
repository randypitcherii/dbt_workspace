{% macro pack_json(from, relation_alias=False, except=[]) -%}
    {%- set include_cols = [] %}
    {%- set cols = adapter.get_columns_in_relation(from) -%}

    {%- for col in cols -%}
        {%- if col.column not in except -%}
            {% do include_cols.append(col.column) %}
        {%- endif %}
    {%- endfor %}

    object_construct_keep_null(
        {%- for col in include_cols %}        
            {%- if relation_alias %}{{ relation_alias }}.{% else %}{%- endif -%}{{"'" + col + "'" + ', ' + col }}
            {%- if not loop.last %},{{'\n  ' }}{% endif %}
        {%- endfor -%}
    )
{%- endmacro %}