{% macro validate_column_names(model) %}
  {% if not execute %}
      {{ return(None) }}
  {% endif %}

  {% set cols = get_column_names(model) %}
  
  {% set has_id_col = [] %}
  {% for col in cols %}
    {% if col.lower().endswith('_id') %}
      {% do has_id_col.append(True) %}
    {% endif %}
  {% endfor %}
  
  {% if not has_id_col %}
    {%- set err -%}
      Model {{model.identifier}} has no id column. 1 column name must end in '_id'. Columns:
      {%- for col in cols -%}
        {{'\n\t'}}{{col}}
      {%- endfor -%}
    {%- endset -%}
    {{ exceptions.raise_compiler_error(err) }}
  {% endif %}
{% endmacro %}