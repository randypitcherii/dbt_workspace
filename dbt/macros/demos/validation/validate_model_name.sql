{% macro validate_model_name(model_to_validate) %}
    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set path=model_to_validate.path %}
    {% set name=model_to_validate.name %}

    {# validate stages have the proper prefix #}
    {% if '/staging/' in path and not name.startswith('stg_') %}
      {{ exceptions.raise_compiler_error("Invalid model name validation. Staging models must start with 'stg_'. Got: " ~ name) }}
    {% endif %}
{% endmacro %}