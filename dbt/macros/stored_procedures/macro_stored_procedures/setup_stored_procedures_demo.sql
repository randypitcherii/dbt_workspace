{% macro setup_stored_procedures_demo(dry_run=True) %}
    {%- set password -%}    
        Aa{{ range(1000) | random }}-{{ range(1000) | random }}-{{ range(1000) | random }}bB
    {%- endset -%}

    {# behold the drowssap! #} 
    {% set setup_sql=get_workspace_setup_script('sp_to_dbt', password, password|reverse ) %}

    {% if dry_run %}
        {% do log(setup_sql, True) %}
    {% else %}
        {% do run_query(setup_sql) %}
    {% endif %}
{% endmacro %}