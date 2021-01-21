{% macro delete_snapshots() %}
    {#
    Run the following dbt command to delete snapshots in this project:
        dbt run-operation delete_snapshots
    #}

    {% set query = 'drop table ' ~target.database~'.'~target.schema~'.hard_deletes__snapshot' %}

    {% if execute %}
        {% do run_query(query) %}
        {% do log('Cleaned up snapshots with following query:\n'~query, True) %}
    {% endif %}
{% endmacro %}