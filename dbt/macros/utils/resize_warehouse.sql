{#    
    Run the following dbt command to delete snapshots in this project:
        dbt run-operation resize_warehouse('my_warehouse','xsmall')
#}
{% macro resize_warehouse(warehouse_name, warehouse_size) %}
    {% set query = 'ALTER WAREHOUSE ' ~ warehouse_name ~ ' SET SIZE=' ~ warehouse_size ~ ';' %}
    {% if execute %}            
        {% do run_query(query) %}        
    {% endif %}
{% endmacro %}