;e{% macro lv_create_table_as(identifier, sql, database_override=None, schema_override=None) %}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set database = database_override if database_override else this.database -%}
    {%- set schema = schema_override if schema_override else this.schema -%}

    {{ sql_header if sql_header is not none }}

    create table
        {{database}}.{{schema}}.{{identifier}}
    as (
        {{ sql }}
    );
    
{% endmacro %}


{% materialization lambda_view, default -%}

  {% set watermark  = config.require('watermark') %}
  {% set unique_key = config.get('unique_key') %}
  {% set historical_database   = config.get('historical_database') if config.get('historical_database') else this.database %}
  {% set historical_schema     = config.get('historical_schema') if config.get('historical_schema') else this.schema %}
  {% set historical_identifier = config.get('historical_identifier') if config.get('historical_identifier') else this.identifier ~ '__historical' %}

  {% set target_relation     = this.incorporate(type='view') %}
  {% set historical_relation = adapter.get_relation(identifier=historical_identifier, schema=historical_schema, database=historical_database) %}
  {% set existing_relation   = load_relation(this) %}
  {% set tmp_relation        = make_temp_relation(this) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop  = [] %}
  {% set to_build = [] %}
  {% if existing_relation is none %}
      {% set historical_build_sql = lv_create_table_as(target_relation.identifier, sql, historical_database, historical_schema) %}
      {% do to_build.append(historical_build_sql) %}

  {% elif existing_relation.is_table or historical_relation.is_view or should_full_refresh() %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      {% do adapter.drop_relation(backup_relation) %}

      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
      {% do to_drop.append(backup_relation) %}
      
  {% else %}
      {% set tmp_relation = make_temp_relation(target_relation) %}

      {% set easy_incremental_sql %}
        {{sql}} 

        {% if watermark is not none %}
            -- this filter will only be applied on an incremental run
            WHERE {{ watermark }} >= (SELECT MAX(THIS.{{ watermark }}) FROM {{ this }} THIS)
        {% endif %}
      {% endset %}

      {% do run_query(create_table_as(True, tmp_relation, easy_incremental_sql)) %}
      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
      {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
      {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
      {% if not dest_columns %}
        {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
      {% endif %}
      {% set build_sql = get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key=unique_key, dest_columns=dest_columns) %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}