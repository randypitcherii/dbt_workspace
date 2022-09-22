{% macro error_on_empty_runs() %}
    {{ log(''~results, info=True) }}
{% endmacro %}