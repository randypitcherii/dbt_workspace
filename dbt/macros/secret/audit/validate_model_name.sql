{% macro validate_model_name(model, ruleset=None) %}

  {% if ruleset == None %}
      {{ return() }}
  {% elif ruleset == 'stage' %}  
    {% if not model.identifier.startswith('stg_') %}
      {{ exceptions.raise_compiler_error("Invalid model name validation. Staging models must start with 'stg_'. Got: " ~ model.identifier) }}
    {% endif %}
  {% else %}  
    {{ exceptions.raise_compiler_error("Invalid model name validation ruleset. Got: " ~ ruleset) }}
  {% endif %}
{% endmacro %}