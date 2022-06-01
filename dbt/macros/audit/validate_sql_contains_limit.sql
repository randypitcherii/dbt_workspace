{% macro validate_sql_contains_limit(model) %}
    {% if not model.raw_sql.endswith('limit 100') %}
      {{ exceptions.raise_compiler_error("Invalid model sql. Model: " ~ model.path ~ " must end with 'limit 100'") }}
    {% endif %}
{% endmacro %}