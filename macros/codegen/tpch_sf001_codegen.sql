{% macro tpch_sf001_codegen() %}

    {#
    Run the following dbt command:
        dbt run-operation tpch_sf001_codegen
    
    Then, copy the output into your source.yml file!
    #}

    {{ codegen.generate_source(schema_name='TPCH_SF001', database_name='RAW', generate_columns=True) }}

{% endmacro %}