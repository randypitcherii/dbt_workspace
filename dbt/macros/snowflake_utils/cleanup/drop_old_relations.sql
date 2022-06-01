{# drop_old_relations

macros:
  - name: drop_old_relations
    description: 'This macro deletes all tables and views that have not been modified in a given number of hours'
    arguments:
      - name: age_cutoff_in_hours
        description: The cutoff in hours since last relation modification. Any relation that has not been modified in this many hours is dropped.

Args:
    - age_cutoff_in_hours: int        -- The cutoff in hours since last relation modification. Any relation that has not been modified in this many hours is dropped.

Example:
    dbt run-operation drop_old_relations --args '{cutoff_in_hours: 96" }'

#}


{% macro drop_old_relations(cutoff_in_hours) %}

  {% set cleanup_query %}

      WITH 

      MODELS_TO_DROP AS (
        SELECT
          CASE 
            WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE'
            WHEN TABLE_TYPE = 'VIEW' THEN 'VIEW'
          END AS RELATION_TYPE,
          CONCAT_WS('.', TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) AS RELATION_NAME
        FROM 
          {{ target.database }}.INFORMATION_SCHEMA.TABLES
        WHERE 
          TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
          AND 
          LAST_ALTERED < DATEADD('HOUR', -{{ cutoff_in_hours }}, CURRENT_TIMESTAMP)
      )

      SELECT 
        'DROP ' || RELATION_TYPE || ' ' || RELATION_NAME || ';' as DROP_COMMANDS
      FROM 
        MODELS_TO_DROP

  {% endset %}

  {% set drop_commands = run_query(cleanup_query).columns[0].values() %}

  {% if drop_commands %}
    {% for drop_command in drop_commands %}
      {% do log(drop_command, True) %}
      {% do run_query(drop_command) %}
    {% endfor %}
  {% else %}
    {% do log('No relations to clean.', True) %}
  {% endif %}

{% endmacro %}
