{#
Run the following dbt command:

    dbt run-operation generate_source_yml_for_database --args '{db: my_db, generate_cols: True}'

Then, copy the output into your source.yml file!
#}

{% macro generate_source_yml_for_database(db='RAW', generate_cols=True) %}

    {# Get schemas (technically, schemata is the plural of schema - but I am a rebel) #}
    {% set schemas = run_query("select schema_name from " ~ db ~ ".information_schema.schemata where schema_name != 'INFORMATION_SCHEMA'").columns[0].values() %}

    {# Generate source yaml for each database. There are duplicate version entries for now but I would like to fix this in the future. For now, just buck up and clean the text by hand #}
    {% for schema in schemas %}
        {{ codegen.generate_source(schema_name=schema, database_name=db, generate_columns=generate_cols) }}
        {% do log('\n---\n\n', true) %}
    {% endfor %}

{% endmacro %}