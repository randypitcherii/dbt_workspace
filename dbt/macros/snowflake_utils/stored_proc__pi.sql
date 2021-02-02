{% macro stored_proc__pi() %}

    {% set stored_proc_name %}
        {{target.database}}.{{target.schema}}.sp_pi
    {% endset %}
    
    {% set stored_proc_ddl_query %}
        create or replace procedure {{stored_proc_name}}()
        returns float not null
        language javascript
        as
        $$
            return 3.1415926;
        $$
        ;
    {% endset %}

    {% do run_query(stored_proc_ddl_query) %}
    {% do log('Created stored proc:\t' ~ stored_proc_name, True) %}

    {# return the name of the stored procedure #}
    {%- do return(stored_proc_name) %}
    
{% endmacro %}