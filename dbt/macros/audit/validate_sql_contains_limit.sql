{% macro validate_sql_contains_limit(model) %}
    {% if model.language == 'sql' and not model.raw_code.endswith('limit 100') %}
      {{ exceptions.raise_compiler_error("Invalid model sql. Model: " ~ model.path ~ " must end with 'limit 100'") }}
    {% endif %}
{% endmacro %}