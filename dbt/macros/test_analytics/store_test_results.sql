{% macro store_test_results(results) %}
  {# --add "{{ store_test_results(results) }}" to an on-run-end: block in dbt_project.yml #}
  {# --run with dbt build --store-failures. The next v.1.0.X release of dbt will include post run hooks for dbt test! #}
  {%- set test_results = [] -%}

  {%- for result in results -%}
    {%- if result.node.resource_type == 'test' -%}
      {%- do test_results.append(result) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set central_tbl -%} {{ target.schema }}.test_results_central {%- endset -%}
  {%- set history_tbl -%} {{ target.schema }}.test_results_history {%- endset -%}
  
  {{ log("Centralizing test data in " + central_tbl, info = true) if execute }}

  create or replace table {{ central_tbl }} as (
  
  {% for result in test_results %}
    
    select
      '{{ result.node.name }}' as test_name,
      '{{ result.node.unique_id }}' as model_name,
      '{{ result.node.config.severity }}' as test_severity_config,
      '{{ result.execution_time }}' as execution_time_seconds,
      '{{ result.status }}' as test_result,
      current_timestamp as _timestamp
    
    {{ "union all" if not loop.last }}
  
  {% endfor %}
  
  );

  {% if target.name != 'default' %}
      create table if not exists {{ history_tbl }} as (
        select 
          {{ dbt_utils.surrogate_key(["test_name", "test_result", "_timestamp"]) }} as sk_id, 
          * 
        from {{ central_tbl }}
        where false
      );

    insert into {{ history_tbl }} 
      select 
       {{ dbt_utils.surrogate_key(["test_name", "test_result", "_timestamp"]) }} as sk_id, 
       * 
      from {{ central_tbl }}
    ;
  {% endif %}

{% endmacro %}