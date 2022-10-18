{# drop_deprecated_tables

The purpose of this macro is to remove any stale tables/views that no longer have a corresponding model in dbt project.

Args:
    - dry_run: bool  -- dry run flag. When dry_run is true, the cleanup commands are printed to stdout rather than executed. This is true by default

Example 1 - dry run of current database
    dbt run-operation drop_deprecated_tables    

Example 3 - full clean :)
    dbt run-operation drop_deprecated_tables --args '{dry_run: False}'
#}

{% macro drop_deprecated_tables(dry_run) %}

    {% if execute %}

        {% set current_models=[] %}

        {% for node in graph.nodes.values()
            | selectattr("resource_type", "in", ["model", "seed", "snapshot"])%}
            {% do current_models.append(node.name) %}
        {% endfor %}

    {% endif %}

    {% set cleanup_query %}

        with 
        models_to_drop as (
            select
                case
                    when table_type = 'BASE TABLE' then 'TABLE'
                    when table_type = 'VIEW' then 'VIEW'
                end as relation_type,
                concat_ws('.', table_catalog, table_schema, table_name) as relation_name
            from
                {{ target.database }}.information_schema.tables
            where 
                table_schema ilike '{{ target.schema }}%'
                and table_name not in
                    (
                        {%- for model in current_models -%}
                            '{{ model.upper() }}'
                            {%- if not loop.last -%},{% endif %}
                        {%- endfor -%}
                    )
        )

        select
            'drop ' || relation_type || ' ' || relation_name || ';' as drop_commands
        from
            models_to_drop
        -- intentionally exclude unhandled table_types, including 'external table`
        where drop_commands is not null

    {% endset %}

    {% do log(cleanup_query, info=True) %}

    {% set drop_commands = run_query(cleanup_query).columns[0].values() %}

    {% if drop_commands %}
        {% for drop_command in drop_commands %}

            {% do log(drop_command, True) %}
            {% if dry_run == false %}
                {% do run_query(drop_command) %}
                {% do log('Deprecated Tables Dropped.', True) %}
            {% endif %}

        {% endfor %}

        {% else %}
        {% do log('No relations to clean.', True) %}

    {% endif %}

{%- endmacro -%}
