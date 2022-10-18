
{% materialization partitioned_incremental, default -%}

  {% set unique_key = config.get('unique_key') %}
  {% set partition_by  = config.get('partition_by') %}
  {% set partitions_to_process  = config.get('partitions_to_process') %}

  {% do log('\nunique_key=' ~unique_key~ '\npartition_by=' ~partition_by~ '\npartitions_to_process=' ~partitions_to_process, False)%}

  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or should_full_refresh() %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      {% do adapter.drop_relation(backup_relation) %}

      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
      {% do to_drop.append(backup_relation) %}
  {% else %}
      {% set tmp_relation = make_temp_relation(target_relation) %}

      {% set partitioned_incremental_sql %}
        {{sql}} 

        {% if partition_by and partitions_to_process %}
            -- this filter will only be applied on an incremental run
            WHERE {{ partition_by }} IN ( 
                {% for partition_to_process in partitions_to_process %}
                    {{partition_to_process}}{% if not loop.last %},{% endif %}
                {% endfor %}
            )
        {% endif %}
      {% endset %}

      {% do log('\n\n'~partitioned_incremental_sql~'\n\n', False)%}


      {% do run_query(create_table_as(True, tmp_relation, partitioned_incremental_sql)) %}
      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
      {% set build_sql = incremental_upsert(tmp_relation, target_relation, unique_key=unique_key) %}
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