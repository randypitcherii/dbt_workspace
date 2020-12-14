{{ config(tags=["snowflake_meta", "daily"], materialized='incremental', transient=true) }}

WITH HISTORY AS (
  SELECT 
    {{ standardize_timestamp('START_TIME') }} AS START_TIME_CENTRAL_TIME,
    {{ standardize_timestamp('END_TIME') }}   AS END_TIME_CENTRAL_TIME,

	{{ 
      dbt_utils.star(
        from=source('snowflake_meta', 'warehouse_metering_history'),
        except=[
          "START_TIME",
          "END_TIME",
          "INGESTION_TIME"
        ]
      ) 
    }}, 

    END_TIME AS WATERMARK

  FROM 
    {{ source('snowflake_meta', 'warehouse_metering_history') }}

  {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE 
      WATERMARK > (SELECT MAX(THIS.WATERMARK) FROM {{ this }} THIS)
  {% endif %}
)

SELECT * FROM HISTORY