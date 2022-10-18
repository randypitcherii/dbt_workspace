
{% materialization raw_sql, default -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement("main") %}
      {{ sql }}
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}
  
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': []}) }}

{%- endmaterialization %}