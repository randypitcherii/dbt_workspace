{% macro get_dynamic_sql_value() %}

  {% set query = 'select 123' %}

  {% if execute %}
    {% set result = run_query(query).columns[0].values()[0] %}
    {{ return(result) }}
  {% else %}
    {{ return('NULL')}}
  {% endif %}
{% endmacro %}