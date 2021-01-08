{# #}

{% macro create_snapshot_manually(relation_to_snapshot) %}

    {% set snapshot_target = target.database ~ '.' ~ target.schema ~ '.' ~ relation_to_snapshot.split('.')[-1] ~ '__SNAPSHOT' %}

    {% set create_snapshot_query %}
        CREATE TABLE IF NOT EXISTS {{snapshot_target}} AS (
            SELECT 
                *,
                CURRENT_TIMESTAMP AS SNAPSHOTTED_AT_TIME
            FROM {{relation_to_snapshot}}
            LIMIT 0
        )
    {% endset %}


    {% set insert_snapshot_query %}
        INSERT INTO {{snapshot_target}} (
            SELECT 
                *,
                CURRENT_TIMESTAMP AS SNAPSHOTTED_AT_TIME
            FROM {{relation_to_snapshot}}
        )
    {% endset %}

    {% do log('Creating snapshot target table --> ' ~ snapshot_target, True) %}
    {% do run_query(create_snapshot_query) %}

    {% do log('Inserting full snapshot into target table from relation to snapshot --> ' ~ relation_to_snapshot, True) %}
    {% do run_query(insert_snapshot_query) %}

    {% do run_query('COMMIT') %}
  
{% endmacro %}