{{ config(tags=["snowflake_meta", "daily"], materialized='view', transient=true) }}

WITH HISTORY AS (
  SELECT 
    *

  FROM 
  {% if target.name == 'prod' %}
    {{ source('snowflake_meta', 'query_history') }}
  {% elif target.name == 'qa' %}
    {{ source('snowflake_meta_qa', 'query_history') }}
  {% else %}
    {{ source('snowflake_meta_dev', 'query_history') }}
  {% endif %}
)

SELECT * FROM HISTORY