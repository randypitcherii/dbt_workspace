{{ config(tags=["snowflake_meta", "daily"], materialized='incremental', transient=true) }}

SELECT 
    *,
    CURRENT_TIMESTAMP AS SNAPSHOTTED_AT_TIME 
    
FROM {{ref('stg_warehouse_metering_history')}}