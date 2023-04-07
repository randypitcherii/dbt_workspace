-- ==========================================================================
-- This procedure creates several tables and populates them with data from 
-- the Snowflake ACCOUNT_USAGE views. 
--
-- This process includes cursor-based CDC and snapshotting functionality to 
-- ensure that only new data is inserted into the tables. 
-- ==========================================================================
CREATE OR REPLACE 
PROCEDURE RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.PROCESS_SNOWFLAKE_USAGE_DATA()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  //=========================================================================
  // initial schema setup
  //=========================================================================
  // schema
  snowflake.execute({sqlText: "CREATE SCHEMA IF NOT EXISTS RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS;"});

  // warehouse metering history
  snowflake.execute({sqlText: `CREATE TABLE IF NOT EXISTS RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.WAREHOUSE_METERING_HISTORY
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
  snowflake.execute({sqlText: `CREATE TABLE IF NOT EXISTS RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.SNOWPIPES
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
    );`});

  // snowpipe usage history
  snowflake.execute({sqlText: `CREATE TABLE IF NOT EXISTS RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.SNOWPIPE_USAGE_HISTORY
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
    );`});

  // query history
  snowflake.execute({sqlText: `CREATE TABLE IF NOT EXISTS RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.QUERY_HISTORY
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
    );`});

  // Tasks history
  snowflake.execute({sqlText: `CREATE TABLE IF NOT EXISTS RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.TASKS (
    CREATED_ON TIMESTAMP_LTZ,
    NAME STRING,
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    OWNER STRING,
    COMMENT STRING,
    WAREHOUSE STRING,
    SCHEDULE STRING,
    PREDECESSOR STRING,
    STATE STRING,
    DEFINITION STRING,
    CONDITION STRING,
    INGESTION_TIME TIMESTAMP_LTZ
  );`});

  // Task usage history
  snowflake.execute({sqlText: `CREATE TABLE IF NOT EXISTS RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.TASK_USAGE_HISTORY(
    QUERY_ID STRING,
    NAME STRING,
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    QUERY_TEXT STRING,
    CONDITION_TEXT STRING,
    STATE STRING,
    ERROR_CODE STRING,
    ERROR_MESSAGE STRING,
    SCHEDULED_TIME TIMESTAMP_LTZ,
    QUERY_START_TIME TIMESTAMP_LTZ,
    NEXT_SCHEDULED_TIME TIMESTAMP_LTZ,
    COMPLETED_TIME TIMESTAMP_LTZ,
    ROOT_TASK_ID STRING,
    GRAPH_VERSION NUMBER,
    RUN_ID NUMBER,
    RETURN_VALUE STRING,
    INGESTION_TIME TIMESTAMP_LTZ
  );`});
  //=========================================================================


  //=========================================================================
  // account_usage cdc and snapshotting
  //=========================================================================
  // warehouse metering history
  const warehouseCursor = snowflake.execute({sqlText: `SELECT COALESCE(MAX(START_TIME), '1970-01-01'::TIMESTAMP_LTZ) AS CURSOR FROM RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.WAREHOUSE_METERING_HISTORY;`});
  const warehouseCursorResult = warehouseCursor.next();
  const warehouseCursorValue = warehouseCursorResult.CURSOR;

  snowflake.execute({sqlText: `INSERT INTO RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.WAREHOUSE_METERING_HISTORY
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
      TABLE(SYSTEM$ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY(
        START_TIME_RANGE_START => '${warehouseCursorValue}',
        END_TIME_RANGE_START => '${warehouseCursorValue}'
      ));`});

  // snowpipe history
  const snowpipeCursor = snowflake.execute({
    sqlText: `SELECT COALESCE(MAX(CREATED), '1970-01-01'::TIMESTAMP_LTZ) AS CURSOR FROM RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.SNOWPIPES;`
  });
  const snowpipeCursorResult = snowpipeCursor.next();
  const snowpipeCursorValue = snowpipeCursorResult.CURSOR;

  snowflake.execute({sqlText: `INSERT INTO RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.SNOWPIPES
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
      TABLE(SYSTEM$ACCOUNT_USAGE.PIPES(
        CREATED_RANGE_START => '${snowpipeCursorValue}',
        LAST_ALTERED_RANGE_START => '${snowpipeCursorValue}',
        SHOW_DELETED_OBJECTS => TRUE
      ));`});

  // snowpipe usage history
  const pipeUsageCursor = snowflake.execute({
    sqlText: `SELECT COALESCE(MAX(END_TIME), '1970-01-01'::TIMESTAMP_LTZ) AS CURSOR FROM RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.SNOWPIPE_USAGE_HISTORY;`
  });
  const pipeUsageCursorResult = pipeUsageCursor.next();
  const pipeUsageCursorValue = pipeUsageCursorResult.CURSOR;

  snowflake.execute({sqlText: `INSERT INTO RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.SNOWPIPE_USAGE_HISTORY
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
      END_TIME > '${snowpipeCursorValue}';`});

  // query history
  const queryCursor = snowflake.execute({sqlText: `SELECT COALESCE(MAX(END_TIME), '1970-01-01'::TIMESTAMP_LTZ) AS CURSOR FROM RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.QUERY_HISTORY;`});
  const queryCursorResult = queryCursor.next();
  const queryCursorValue = queryCursorResult.CURSOR;

  snowflake.execute({sqlText: `INSERT INTO RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.QUERY_HISTORY
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
      END_TIME > '${queryCursorValue}';`});
      
  // Tasks history
  const taskCursor = snowflake.execute({sqlText: `SELECT COALESCE(MAX(INGESTION_TIME), '1970-01-01'::TIMESTAMP_LTZ) AS CURSOR FROM RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.TASKS;`});
  const taskCursorResult = taskCursor.next();
  const taskCursorValue = taskCursorResult.CURSOR;

  snowflake.execute({sqlText: `INSERT INTO RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.TASKS
    SELECT
      CREATED_ON,
      NAME,
      DATABASE_NAME,
      SCHEMA_NAME,
      OWNER,
      COMMENT,
      WAREHOUSE,
      SCHEDULE,
      PREDECESSOR,
      STATE,
      DEFINITION,
      CONDITION,
      CURRENT_TIMESTAMP AS INGESTION_TIME
    FROM
      SNOWFLAKE.ACCOUNT_USAGE.TASKS
    WHERE
      INGESTION_TIME > '${taskCursorValue}';`});

  // Task usage history
  const taskUsageCursor = snowflake.execute({
    sqlText: `SELECT COALESCE(MAX(INGESTION_TIME), '1970-01-01'::TIMESTAMP_LTZ) AS CURSOR FROM RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.TASK_USAGE_HISTORY;`
  });
  const taskUsageCursorResult = taskUsageCursor.next();
  const taskUsageCursorValue = taskUsageCursorResult.CURSOR;

  snowflake.execute({sqlText: `INSERT INTO RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCS.TASK_USAGE_HISTORY
    SELECT
      QUERY_ID,
      NAME,
      DATABASE_NAME,
      SCHEMA_NAME,
      QUERY_TEXT,
      CONDITION_TEXT
      STATE,
      ERROR_CODE,
      ERROR_MESSAGE,
      SCHEDULED_TIME,
      QUERY_START_TIME,
      NEXT_SCHEDULED_TIME,
      COMPLETED_TIME,
      ROOT_TASK_ID,
      GRAPH_VERSION,
      RUN_ID,
      RETURN_VALUE,
      CURRENT_TIMESTAMP AS INGESTION_TIME
    FROM
      SNOWFLAKE.ACCOUNT_USAGE.TASK_USAGE_HISTORY
    WHERE
      INGESTION_TIME > '${taskUsageCursorValue}';`});
  //=========================================================================


  return "Success";
$$;

