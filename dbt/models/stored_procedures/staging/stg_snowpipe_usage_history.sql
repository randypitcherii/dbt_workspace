SELECT
    PIPE_ID,
    PIPE_NAME,
    START_TIME,
    END_TIME,
    CREDITS_USED,
    BYTES_INSERTED,
    FILES_INSERTED,
    CURRENT_TIMESTAMP AS INGESTION_TIME

FROM {{ source('snowflake_account_usage', 'pipe_usage_history') }}