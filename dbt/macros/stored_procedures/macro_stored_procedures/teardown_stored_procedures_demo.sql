{% macro teardown_stored_procedures_demo(project_name='sp_to_dbt', dry_run=True) %}
    {# goodnight sweet prince #} 
    {% set sql=get_workspace_teardown_script(project_name) %}

    {% if dry_run %}
        {% do log(sql, True) %}
    {% else %}
        {% do run_query(sql) %}
    {% endif %}

    {{ return(sql) }}
{% endmacro %}