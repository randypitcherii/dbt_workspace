{% macro get_stage_columns_from_loading_config(stage_name, config_table) %}
    {#- get column info from config table -#}
    {% set get_ddl_portions_query %}
        with 
        
        stage_cols as (
            select *
            from {{config_table}}
            where stage_name = '{{stage_name}}'
        ),

        latest_stage_columns as (
            select * 
            from stage_cols
            where updated_at = (select max(updated_at) from stage_cols)
        ),

        ddl_portions as (
            select
                '$' || column_position || '::' || column_type || ' as ' || column_name as ddl_portions
            from latest_stage_columns
        )

        select * from ddl_portions
    {% endset %}

    {% if execute %}
        {% set ddl_portions = run_query(get_ddl_portions_query).columns[0].values() %}
        {% for ddl_portion in ddl_portions %}
          {{ddl_portion}} {% if not loop.last %}, {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}






