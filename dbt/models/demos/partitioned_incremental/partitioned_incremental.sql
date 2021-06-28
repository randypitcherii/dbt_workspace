{{ 
    config(
        materialized='partitioned_incremental', 
        partition_by='WAREHOUSE_NAME', 
        partitions_to_process=["'RANDY_PITCHER_WORKSPACE_PROD_WH'", "'RANDY_PITCHER_WORKSPACE_DEV_WH'"],
        unique_key='record_id'
    ) 
}}

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

    END_TIME AS WATERMARK,
    {{dbt_utils.surrogate_key(['START_TIME', 'WAREHOUSE_NAME'])}} AS record_id

  FROM 
    {{ source('snowflake_meta', 'warehouse_metering_history') }}
)

SELECT * FROM HISTORY

-- {{this}} {{target.database}}