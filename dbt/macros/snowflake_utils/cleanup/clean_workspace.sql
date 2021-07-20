{# clean_workspace

This macro drops all the schemas within a database to "clean" the workspace. Use the dry_run param to see the schemas that will be dropped before dropping them.
The schemas to drop will be those that match the schema_like (if provided) AND do not match the schema_not_like (if provided). The information_schema is always
excluded as it is not droppable in snowflake. 

Args:
    - database: string        -- the name of the database to clean. By default the target.database is used
    - dry_run: bool           -- dry run flag. When dry_run is true, the cleanup commands are printed to stdout rather than executed. This is true by default
    - schema_like: string     -- case-insensitive like pattern of schema names to include. This is None by default. 
    - schema_not_like: string -- case-insensitive like pattern of schema names to exclude. This is None by default 

Example 1 - dry run of current database
    dbt run-operation clean_workspace    
    
Example 2 - drop any schemas in a given database that match a given schema_like string
    dbt run-operation clean_workspace --args '{database: my_database, dry_run: False, schema_like: "cool_schema_%" }'
    
Example 3 - full clean :)
    dbt run-operation clean_workspace --args '{dry_run: False}'
#}

{% macro clean_workspace(database=target.database, dry_run=True, schema_like=None, schema_not_like=None) %}
    {%- set msg -%}
        Starting clean_workspace...
          database:        {{database}} 
          dry_run:         {{dry_run}} 
          schema_like:     {{schema_like}} 
          schema_not_like: {{schema_not_like}} 
    {%- endset -%}
    {{ log(msg, info=True) }}


    {% set get_drop_commands_query %}
        SELECT
            'DROP SCHEMA {{database}}.' || SCHEMA_NAME || ';' AS DROP_QUERY
        FROM
            {{database}}.INFORMATION_SCHEMA.SCHEMATA
        WHERE
            SCHEMA_NAME != 'INFORMATION_SCHEMA'
        {%- if schema_like -%}
            AND SCHEMA_NAME ILIKE '{{schema_like}}' 
        {%- endif -%}
        {%- if schema_not_like -%}
            AND NOT SCHEMA_NAME ILIKE '{{schema_not_like}}' 
        {%- endif -%}
    {% endset %}


    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[0].values() %}


    {% for drop_query in drop_queries %}
        {% if execute and not dry_run %}
            {{ log('Dropping schema with command: ' ~ drop_query, info=True) }}
            {% do run_query(drop_query) %}    
        {% else %}
            {{ log(drop_query, info=True) }}
        {% endif %}
    {% endfor %}
{% endmacro %}