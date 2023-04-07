{% macro build_and_process_stored_procedures_demo(database, role, warehouse, dry_run=True) %}
    {% set sql %}
        -- schema
        USE ROLE {{role}};
        USE WAREHOUSE {{warehouse}};
        CREATE SCHEMA IF NOT EXISTS {{database}}.STORED_PROCEDURES;
        CREATE SCHEMA IF NOT EXISTS {{database}}.STORED_PROCEDURES_MART;

        -- warehouse metering history
        CREATE TABLE IF NOT EXISTS {{database}}.STORED_PROCEDURES.WAREHOUSE_METERING_HISTORY
        AS (
        SELECT
            START_TIME,
            END_TIME,
            WAREHOUSE_ID,
            WAREHOUSE_NAME,
            CREDITS_USED,
            CREDITS_USED_COMPUTE,
            CREDITS_USED_CLOUD_SERVICES,
            CURRENT_TIMESTAMP AS INGESTION_TIME
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        );

        -- snowpipe history
        CREATE TABLE IF NOT EXISTS {{database}}.STORED_PROCEDURES.SNOWPIPES
        AS (
        SELECT
            PIPE_ID,
            PIPE_NAME,
            PIPE_SCHEMA_ID,
            PIPE_SCHEMA,
            PIPE_CATALOG_ID,
            PIPE_CATALOG,
            IS_AUTOINGEST_ENABLED,
            NOTIFICATION_CHANNEL_NAME,
            PIPE_OWNER,
            DEFINITION,
            CREATED,
            LAST_ALTERED,
            COMMENT,
            DELETED,
            CURRENT_TIMESTAMP AS INGESTION_TIME
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.PIPES
        );

        -- snowpipe usage history
        CREATE TABLE IF NOT EXISTS {{database}}.STORED_PROCEDURES.SNOWPIPE_USAGE_HISTORY
        AS (
        SELECT
            PIPE_ID,
            PIPE_NAME,
            START_TIME,
            END_TIME,
            CREDITS_USED,
            BYTES_INSERTED,
            FILES_INSERTED,
            CURRENT_TIMESTAMP AS INGESTION_TIME
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
        );

        -- query history
        CREATE TABLE IF NOT EXISTS {{database}}.STORED_PROCEDURES.QUERY_HISTORY
        AS (
        SELECT
            QUERY_ID,
            QUERY_TEXT,
            DATABASE_ID,
            DATABASE_NAME,
            SCHEMA_ID,
            SCHEMA_NAME,
            QUERY_TYPE,
            SESSION_ID,
            USER_NAME,
            ROLE_NAME,
            WAREHOUSE_ID,
            WAREHOUSE_NAME,
            WAREHOUSE_SIZE,
            WAREHOUSE_TYPE,
            CLUSTER_NUMBER,
            QUERY_TAG,
            EXECUTION_STATUS,
            ERROR_CODE,
            ERROR_MESSAGE,
            START_TIME,
            END_TIME,
            TOTAL_ELAPSED_TIME,
            BYTES_SCANNED,
            PERCENTAGE_SCANNED_FROM_CACHE,
            BYTES_WRITTEN,
            BYTES_WRITTEN_TO_RESULT,
            BYTES_READ_FROM_RESULT,
            ROWS_PRODUCED,
            ROWS_INSERTED,
            ROWS_UPDATED,
            ROWS_DELETED,
            ROWS_UNLOADED,
            BYTES_DELETED,
            PARTITIONS_SCANNED,
            PARTITIONS_TOTAL,
            BYTES_SPILLED_TO_LOCAL_STORAGE,
            BYTES_SPILLED_TO_REMOTE_STORAGE,
            BYTES_SENT_OVER_THE_NETWORK,
            COMPILATION_TIME,
            EXECUTION_TIME,
            QUEUED_PROVISIONING_TIME,
            QUEUED_REPAIR_TIME,
            QUEUED_OVERLOAD_TIME,
            TRANSACTION_BLOCKED_TIME,
            OUTBOUND_DATA_TRANSFER_CLOUD,
            OUTBOUND_DATA_TRANSFER_REGION,
            OUTBOUND_DATA_TRANSFER_BYTES,
            INBOUND_DATA_TRANSFER_CLOUD,
            INBOUND_DATA_TRANSFER_REGION,
            INBOUND_DATA_TRANSFER_BYTES,
            LIST_EXTERNAL_FILES_TIME,
            CREDITS_USED_CLOUD_SERVICES,
            CURRENT_TIMESTAMP AS INGESTION_TIME
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        );

        -- Task versions
        CREATE TABLE IF NOT EXISTS {{database}}.STORED_PROCEDURES.TASK_VERSIONS AS (
        SELECT
            ROOT_TASK_ID,
            GRAPH_VERSION,
            GRAPH_VERSION_CREATED_ON,
            NAME,
            ID,
            DATABASE_ID,
            DATABASE_NAME,
            SCHEMA_ID,
            SCHEMA_NAME,
            OWNER,
            COMMENT,
            WAREHOUSE_NAME,
            SCHEDULE,
            PREDECESSORS,
            STATE,
            DEFINITION,
            CONDITION_TEXT,
            ALLOW_OVERLAPPING_EXECUTION,
            ERROR_INTEGRATION,
            LAST_COMMITTED_ON,
            LAST_SUSPENDED_ON,
            CURRENT_TIMESTAMP AS INGESTION_TIME
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.TASK_VERSIONS
        );

        -- Task history
        CREATE TABLE IF NOT EXISTS {{database}}.STORED_PROCEDURES.TASK_HISTORY AS (
        SELECT
            NAME,
            QUERY_TEXT,
            CONDITION_TEXT,
            SCHEMA_NAME,
            TASK_SCHEMA_ID,
            DATABASE_NAME,
            TASK_DATABASE_ID,
            SCHEDULED_TIME,
            COMPLETED_TIME,
            STATE,
            RETURN_VALUE,
            QUERY_ID,
            QUERY_START_TIME,
            ERROR_CODE,
            ERROR_MESSAGE,
            GRAPH_VERSION,
            RUN_ID,
            ROOT_TASK_ID,
            SCHEDULED_FROM,
            CURRENT_TIMESTAMP AS INGESTION_TIME
        FROM
            SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
        );

        -- Create a table to store warehouse metering history and grant ownership and select permissions.
        CREATE OR REPLACE TABLE 
        {{database}}.STORED_PROCEDURES_MART.WAREHOUSE_METERING_HISTORY AS (
            SELECT * 
            FROM {{database}}.STORED_PROCEDURES.WAREHOUSE_METERING_HISTORY
        );
        GRANT OWNERSHIP ON TABLE {{database}}.STORED_PROCEDURES_MART.WAREHOUSE_METERING_HISTORY TO ROLE {{database}}_WRITE COPY CURRENT GRANTS;
        GRANT SELECT ON TABLE {{database}}.STORED_PROCEDURES_MART.WAREHOUSE_METERING_HISTORY TO ROLE {{database}}_READ;

        -- Create a table to store snowpipes and grant ownership and select permissions.
        CREATE OR REPLACE TABLE {{database}}.STORED_PROCEDURES_MART.SNOWPIPES AS (
            SELECT 
                *, 
                (SELECT MAX(INGESTION_TIME) FROM {{database}}.STORED_PROCEDURES.SNOWPIPES) AS LATEST_INGESTION_TIME, 
                INGESTION_TIME = LATEST_INGESTION_TIME AS IS_LATEST 
            FROM {{database}}.STORED_PROCEDURES.SNOWPIPES
        );
        GRANT OWNERSHIP ON TABLE {{database}}.STORED_PROCEDURES_MART.SNOWPIPES TO ROLE {{database}}_WRITE COPY CURRENT GRANTS;
        GRANT SELECT ON TABLE {{database}}.STORED_PROCEDURES_MART.SNOWPIPES TO ROLE {{database}}_READ;

        -- Create a table to store snowpipe usage history and grant ownership and select permissions.
        CREATE OR REPLACE TABLE {{database}}.STORED_PROCEDURES_MART.SNOWPIPE_USAGE_HISTORY AS (
            SELECT * 
            FROM {{database}}.STORED_PROCEDURES.SNOWPIPE_USAGE_HISTORY
        );
        GRANT OWNERSHIP ON TABLE {{database}}.STORED_PROCEDURES_MART.SNOWPIPE_USAGE_HISTORY TO ROLE {{database}}_WRITE COPY CURRENT GRANTS;
        GRANT SELECT ON TABLE {{database}}.STORED_PROCEDURES_MART.SNOWPIPE_USAGE_HISTORY TO ROLE {{database}}_READ;

        -- Create a table to store query history and grant ownership and select permissions.
        CREATE OR REPLACE TABLE {{database}}.STORED_PROCEDURES_MART.QUERY_HISTORY AS (
            SELECT * 
            FROM {{database}}.STORED_PROCEDURES.QUERY_HISTORY
        );
        GRANT OWNERSHIP ON TABLE {{database}}.STORED_PROCEDURES_MART.QUERY_HISTORY TO ROLE {{database}}_WRITE COPY CURRENT GRANTS;
        GRANT SELECT ON TABLE {{database}}.STORED_PROCEDURES_MART.QUERY_HISTORY TO ROLE {{database}}_READ;

        -- Create a table to store tasks and grant ownership and select permissions.
        CREATE OR REPLACE TABLE {{database}}.STORED_PROCEDURES_MART.TASKS AS (
        SELECT 
            *, 
            (SELECT MAX(INGESTION_TIME) FROM {{database}}.STORED_PROCEDURES.TASK_VERSIONS) AS LATEST_INGESTION_TIME, 
            INGESTION_TIME = LATEST_INGESTION_TIME AS IS_LATEST 
        FROM {{database}}.STORED_PROCEDURES.TASK_VERSIONS
        );
        GRANT OWNERSHIP ON TABLE {{database}}.STORED_PROCEDURES_MART.TASKS TO ROLE {{database}}_WRITE COPY CURRENT GRANTS;
        GRANT SELECT ON TABLE {{database}}.STORED_PROCEDURES_MART.TASKS TO ROLE {{database}}_READ;

        -- Create a table to store task usage history and grant ownership and select permissions. 
        CREATE OR REPLACE TABLE {{database}}.STORED_PROCEDURES_MART.TASK_USAGE_HISTORY AS ( 
            SELECT * 
            FROM {{database}}.STORED_PROCEDURES.TASK_HISTORY 
        ); 
        GRANT OWNERSHIP ON TABLE {{database}}.STORED_PROCEDURES_MART.TASK_USAGE_HISTORY TO ROLE {{database}}_WRITE COPY CURRENT GRANTS;
        GRANT SELECT ON TABLE {{database}}.STORED_PROCEDURES_MART.TASK_USAGE_HISTORY TO ROLE {{database}}_READ; 

        --Create a table to store snowflake compute costs and grant ownership and select permissions. 
        CREATE OR REPLACE TABLE {{database}}.STORED_PROCEDURES_MART.SNOWFLAKE_COST AS ( 
        SELECT 
            SUM(CREDITS_USED) * 3.00 AS COST, 
            START_TIME::DATE AS START_DATE 
        FROM {{database}}.STORED_PROCEDURES_MART.WAREHOUSE_METERING_HISTORY 
        GROUP BY START_DATE
        ); 
        GRANT OWNERSHIP ON TABLE {{database}}.STORED_PROCEDURES_MART.SNOWFLAKE_COST TO ROLE {{database}}_WRITE COPY CURRENT GRANTS; 
        GRANT SELECT ON TABLE {{database}}.STORED_PROCEDURES_MART.SNOWFLAKE_COST TO ROLE {{database}}_READ;
    {% endset %}


    {% if dry_run %}
        {% do log(sql, False) %}
    {% else %}
        {% do run_query(sql) %}
    {% endif %}

    {{ return(sql) }}
{% endmacro %}