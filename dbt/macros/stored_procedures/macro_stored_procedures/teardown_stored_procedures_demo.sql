{% macro teardown_stored_procedures_demo(dry_run=True) %}
    {# goodnight sweet prince #} 
    {% set teardown_sql=get_workspace_teardown_script('sp_to_dbt') %}

    {% if dry_run %}
        {% do log(teardown_sql, True) %}
    {% else %}
        {% do run_query(teardown_sql) %}
    {% endif %}
{% endmacro %}