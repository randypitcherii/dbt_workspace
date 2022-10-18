{% macro centralize_test_failures(results) %}
  {# --add "{{ centralize_test_failures(results) }}" to an on-run-end: block in dbt_project.yml #}
  {# --run with dbt build --store-failures. The next v.1.0.X release of dbt will include post run hooks for dbt test! #}
  {%- set test_results = [] -%}
  {%- for result in results -%}
    {%- if result.node.resource_type == 'test' and result.status != 'skipped' and (
          result.node.config.get('store_failures') or flags.STORE_FAILURES
      )
    -%}
      {%- do test_results.append(result) -%}
    {%- endif -%}
  {%- endfor -%}
  
  {%- set central_tbl -%} {{ target.schema }}.test_failure_central {%- endset -%}
  {%- set history_tbl -%} {{ target.schema }}.test_failure_history {%- endset -%}
  
  {{ log("Centralizing test failures in " + central_tbl, info = true) if execute }}

  create or replace table {{ central_tbl }} as (
  
  {% for result in test_results %}
    
    select
      '{{ result.node.name }}' as test_name,
      '{{ result.node.unique_id }}' as model_name,
      object_construct_keep_null(*) as test_failures_json,
      current_timestamp as _timestamp
      
    from {{ result.node.relation_name }}
    
    {{ "union all" if not loop.last }}
  
  {% endfor %}
  
  );
  
  -- only run centralization in higher environments
  {% if target.name != 'default' %}
      create table if not exists {{ history_tbl }} as (
        select 
          {{ dbt_utils.surrogate_key(["test_name", "test_failures_json", "_timestamp"]) }} as sk_id, 
          * 
        from {{ central_tbl }}
        where false
      );

    insert into {{ history_tbl }} 
      select 
       {{ dbt_utils.surrogate_key(["test_name", "test_failures_json", "_timestamp"]) }} as sk_id, 
       * 
      from {{ central_tbl }}
    ;
  {% endif %}

{% endmacro %}