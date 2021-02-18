{% macro create_augmented_snapshot(snapshot_table, table_key) %}

    {% if 'snowflake' == target.type %}
        {{ snowflake__create_augmented_snapshot(snapshot_table, table_key) }}
    {% elif 'bigquery' == target.type %}
        {{ bigquery__create_augmented_snapshot(snapshot_table, table_key) }}
    {% else %}
        {% do log('Target type "' ~ target.type ~ '" not supported by the create_augmented_snapshot macro at this time', True) %}
    {% endif %}

{% endmacro %}