-- ==========================================================================
-- This procedure creates several tables and populates them with data from 
-- the Snowflake ACCOUNT_USAGE views. 
--
-- This process includes cursor-based CDC and snapshotting functionality to 
-- ensure that only new data is inserted into the tables. 
-- ==========================================================================
CREATE OR REPLACE PROCEDURE 
RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCEDURES.PROCESS_SNOWFLAKE_USAGE_DATA(
    DATABASE STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try { 
  //=========================================================================
  // initial setup
  //=========================================================================
  // schema
  snowflake.execute({sqlText: `CREATE SCHEMA IF NOT EXISTS ${DATABASE}.STORED_PROCEDURES;`});

  // warehouse metering history
  snowflake.execute({sqlText: `CREATE TABLE IF NOT EXISTS ${DATABASE}.STORED_PROCEDURES.WAREHOUSE_METERING_HISTORY
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
    );`});

  // snowpipe history
  snowflake.execute({sqlText: `
    CREATE TABLE IF NOT EXISTS ${DATABASE}.STORED_PROCEDURES.SNOWPIPES
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
  `});

  // snowpipe usage history
  snowflake.execute({sqlText: `
    CREATE TABLE IF NOT EXISTS ${DATABASE}.STORED_PROCEDURES.SNOWPIPE_USAGE_HISTORY
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
  `});

  // query history
  snowflake.execute({sqlText: `
    CREATE TABLE IF NOT EXISTS ${DATABASE}.STORED_PROCEDURES.QUERY_HISTORY
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
  `});

  // Task versions
  snowflake.execute({sqlText: `
    CREATE TABLE IF NOT EXISTS ${DATABASE}.STORED_PROCEDURES.TASK_VERSIONS AS (
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
  `});

  // Task history
  snowflake.execute({sqlText: `
    CREATE TABLE IF NOT EXISTS ${DATABASE}.STORED_PROCEDURES.TASK_HISTORY AS (
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
  `});
  //=========================================================================


  //=========================================================================
  // account_usage cdc and snapshotting
  //=========================================================================
  // warehouse metering history
  const warehouseCursorResultSet = snowflake.execute({sqlText: `
    SELECT COALESCE(MAX(START_TIME), '1970-01-01'::TIMESTAMP_LTZ) AS CURSOR 
    FROM ${DATABASE}.STORED_PROCEDURES.WAREHOUSE_METERING_HISTORY;
  `});
  warehouseCursorResultSet.next(); // prepare result set to retrieve next value
  const warehouseCursorValue = warehouseCursorResultSet.getColumnValueAsString(1);

  snowflake.execute({sqlText: `
    INSERT INTO ${DATABASE}.STORED_PROCEDURES.WAREHOUSE_METERING_HISTORY
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
    WHERE
      END_TIME > '${warehouseCursorValue}'
  `});


  // snowpipe history
  const snowpipeCursorResultSet = snowflake.execute({sqlText: `
    SELECT COALESCE(MAX(CREATED), '1970-01-01'::TIMESTAMP_LTZ) AS CURSOR 
    FROM ${DATABASE}.STORED_PROCEDURES.SNOWPIPES;
  `});
  snowpipeCursorResultSet.next(); // prepare result set to retrieve next value
  const snowpipeCursorValue = snowpipeCursorResultSet.getColumnValueAsString(1);

  snowflake.execute({sqlText: `
    INSERT INTO ${DATABASE}.STORED_PROCEDURES.SNOWPIPES
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
    WHERE
      LAST_ALTERED > '${snowpipeCursorValue}';
  `});


  // snowpipe usage history
  const pipeUsageCursorResultSet = snowflake.execute({sqlText: `
    SELECT COALESCE(MAX(END_TIME), '1970-01-01'::TIMESTAMP_LTZ) AS CURSOR 
    FROM ${DATABASE}.STORED_PROCEDURES.SNOWPIPE_USAGE_HISTORY;
  `});
  pipeUsageCursorResultSet.next(); // prepare result set to retrieve next value
  const pipeUsageCursorValue = pipeUsageCursorResultSet.getColumnValueAsString(1);

  snowflake.execute({sqlText: `
    INSERT INTO ${DATABASE}.STORED_PROCEDURES.SNOWPIPE_USAGE_HISTORY
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
    WHERE
      END_TIME > '${pipeUsageCursorValue}';
  `});


  // query history
  const queryHistoryCursorResultSet = snowflake.execute({sqlText: `
    SELECT COALESCE(MAX(END_TIME), '1970-01-01'::TIMESTAMP_LTZ) AS CURSOR 
    FROM ${DATABASE}.STORED_PROCEDURES.QUERY_HISTORY;
  `});
  queryHistoryCursorResultSet.next(); // prepare result set to retrieve next value
  const queryHistoryCursorValue = queryHistoryCursorResultSet.getColumnValueAsString(1);

  snowflake.execute({sqlText: `
    INSERT INTO ${DATABASE}.STORED_PROCEDURES.QUERY_HISTORY
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
    WHERE
      END_TIME > '${queryHistoryCursorValue}';
  `});
      

  // Task versions
  const taskCursorResultSet = snowflake.execute({sqlText: `
    SELECT COALESCE(MAX(INGESTION_TIME), '1970-01-01'::TIMESTAMP_LTZ) AS CURSOR 
    FROM ${DATABASE}.STORED_PROCEDURES.TASK_VERSIONS;
  `});
  taskCursorResultSet.next(); // prepare result set to retrieve next value
  const taskCursorValue = taskCursorResultSet.getColumnValueAsString(1);

  snowflake.execute({sqlText: `
    INSERT INTO ${DATABASE}.STORED_PROCEDURES.TASK_VERSIONS
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
    WHERE
      GRAPH_VERSION_CREATED_ON > '${taskCursorValue}';
  `});


  // Task usage history
  const taskUsageCursorResultSet = snowflake.execute({sqlText: `
    SELECT COALESCE(MAX(INGESTION_TIME), '1970-01-01'::TIMESTAMP_LTZ) AS CURSOR 
    FROM ${DATABASE}.STORED_PROCEDURES.TASK_HISTORY;
  `});
  taskUsageCursorResultSet.next(); // prepare result set to retrieve next value
  const taskUsageCursorValue = taskUsageCursorResultSet.getColumnValueAsString(1);

  snowflake.execute({sqlText: `
    INSERT INTO ${DATABASE}.STORED_PROCEDURES.TASK_HISTORY
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
    WHERE
      INGESTION_TIME > '${taskUsageCursorValue}';
  `});
  //=========================================================================


  } catch (err)  {
    var result =  `
      Procedure Failed. 
        Message: ${err.message}
        Stack Trace:
        ${err.stack}
    `;
    
    return result;
  }

  return "Successfully built and processed snowflake usage data";
$$;