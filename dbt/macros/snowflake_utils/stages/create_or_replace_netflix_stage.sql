{# 
    To run this macro as an operation as a dry run, just use the following command:
        dbt run-operation create_or_replace_netflix_stage

    To fully execute this macro and run the commands in snowflake, use the following command instead:
        dbt run-operation create_or_replace_netflix_stage --args '{dry_run: False}'

    Read more about running macros as operations here:
        https://docs.getdbt.com/reference/commands/run-operation/

    Args:
        - dry_run -- Default to True - dry run will output the SQL to the logs but won't execute any of it.
#}


{% macro create_or_replace_netflix_stage(dry_run=True) %}

    {# Configure here. You could modify this macro to make these params if you like. #}
    {% set stage_db     = 'randy_pitcher_workspace_raw' %}
    {% set stage_schema = 'netflix_loading' %}
    {% set stage_name   = 'netflix_blob_stage' %}
    {% set azure_url    = 'azure://snowflakestagedemo.blob.core.windows.net/netflix-snowflake-stage' %}
    {% set sf_role      = 'randy_pitcher_workspace_admin' %}

    {% set create_or_replace_query %}
        USE ROLE {{sf_role}};

        -- create the stage
        create or replace stage {{stage_db}}.{{stage_schema}}.{{stage_name}} 
        url='{{azure_url}}'
        FILE_FORMAT= (
            TYPE='CSV'
            SKIP_HEADER=1
        );
    {% endset %}

    {% if dry_run %}
        {% do log('Netflix stage creation dry run :\n' ~ create_or_replace_query, True) %}
    {% else %}
        {% do run_query(create_or_replace_query) %}
    {% endif %}

    {# Always a good idea to explicitly jump back to the default role for the current environment #}
    {% do run_query('USE ROLE ' ~ target.role) %}
{% endmacro %}