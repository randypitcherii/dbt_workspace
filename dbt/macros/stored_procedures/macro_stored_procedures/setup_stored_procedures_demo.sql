{% macro setup_stored_procedures_demo(project_name='sp_to_dbt', dry_run=True) %}
    {%- set password -%}    
        Aa{{ range(1000) | random }}-{{ range(1000) | random }}-{{ range(1000) | random }}bB
    {%- endset -%}

    {# behold the drowssap! #} 
    {% set sql=get_workspace_setup_script(project_name, password, password|reverse ) %}

    {% if dry_run %}
        {% do log(sql, True) %}
    {% else %}
        {% do run_query(sql) %}
    {% endif %}

    {{ return(sql) }}
{% endmacro %}