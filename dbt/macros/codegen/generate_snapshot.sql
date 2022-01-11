{# 
"""
Usage:
{% set tables = codegen.get_tables_in_schema('tpch_sf001', database_name='raw') %}

{% for t in tables %}

{{ gen_snap('analytics', 'dbt_lbondkennedy', 'o_'~t|lower~'key', 'o_'~t|lower~'date', 'tpch', t|lower) }}

{% endfor %}
"""
#}

{% macro generate_snapshot(tgt_db, tgt_schema, u_key, updated_at, src_name, src_table) %}
    
{{ '{% snapshot'}} {{ src_table }}{{'_snapshot %}

{{
    config(
      target_database="'}}{{ tgt_db }}{{'",
      target_schema="'}}{{ tgt_schema }}{{'",
      unique_key="'}}{{ u_key }}{{'",
      strategy="timestamp",
      updated_at="'}}{{ updated_at }}{{'",
    )
}}

select * from {{ source("'}}{{ src_name }}{{'", "'}}{{ src_table }}{{'") }}

{% endsnapshot %}'
}}

{% endmacro %}