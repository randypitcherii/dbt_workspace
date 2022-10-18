{% macro get_column_names(model) %}
  {% set query = 'describe view ' ~ model %}

  {% if execute %}
    {% set cols = run_query(query).columns[0].values() %}
    {{ return(cols) }}
  {% endif %}
{% endmacro %}