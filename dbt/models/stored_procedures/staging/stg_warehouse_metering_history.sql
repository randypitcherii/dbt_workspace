SELECT
    START_TIME,
    END_TIME,
    WAREHOUSE_ID,
    WAREHOUSE_NAME,
    CREDITS_USED,
    CREDITS_USED_COMPUTE,
    CREDITS_USED_CLOUD_SERVICES,
    CURRENT_TIMESTAMP AS INGESTION_TIME
    
FROM {{ source('snowflake_account_usage', 'warehouse_metering_history') }}