{% macro unpack_json(model, variant_column_name) %}

  {% set variant_keys_query %}
    WITH 

    KEYS AS (
      SELECT 
        OBJECT_KEYS({{variant_column_name}})
      FROM {{model}}
    )

    SELECT 
      K.VALUE::STRING
    
    FROM TABLE(FLATTEN(INPUT => (SELECT * FROM KEYS))) K
  {% endset %}

  {% if execute %}
    {% set keys = run_query(variant_keys_query).columns[0].values() %}

    {% for key in keys %}
        GET({{variant_column_name}}, '{{key}}') AS {{key}}{% if not loop.last %}, {% endif %}
    {% endfor %}

  {% else %}
    {{ return('NULL')}}
  {% endif %}


{% endmacro %}
