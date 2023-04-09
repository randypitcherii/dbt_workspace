{% materialization copy_into, adapter='snowflake' %}

    {%- set copy_into_on_error = config.get('copy_into_on_error') -%}

    {% set original_query_tag = set_query_tag() %}

    {%- set identifier = model['alias'] -%}

    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database, type='table') -%}

    {{ run_hooks(pre_hooks) }}


    {% if old_relation is none or not old_relation.is_table or should_full_refresh() %}
        {{ log("Replacing existing relation " ~ old_relation) }}
        {%- call statement('main') -%}
            {{ create_table_as(false, target_relation, sql ~ '\nlimit 0') }}
        {%- endcall -%}
    {% endif %}

    {% if copy_into_on_error == none %}

        {%- call statement('main') -%}
            COPY INTO {{target_relation}} 
            FROM ( 
                {{sql}} 
            )
        {%- endcall -%}

    {% else %}

        {%- call statement('main') -%}
            COPY INTO {{target_relation}} 
            FROM ( 
                {{sql}} 
            )
            ON_ERROR = '{{copy_into_on_error}}'
        {%- endcall -%}

    {% endif %}


    {{ run_hooks(post_hooks) }}

    {% do persist_docs(target_relation, model) %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}