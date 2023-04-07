-- This script creates several tables and grants ownership and select permissions to specific ROLEs.
-- It also creates a new ROLE and grants it select permissions on the tables and views in the schema.
-- Finally, it grants usage permissions on the database and warehouse to the new ROLE.

-- call this procedure as securityadmin

CREATE OR REPLACE PROCEDURE 
RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCEDURES.BUILD_SNOWFLAKE_USAGE_MART(
  DATABASE STRING,
  DRY_RUN BOOLEAN
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  const sqlCommands = [
    // create schema if it does not exist yet.
    `CREATE SCHEMA IF NOT EXISTS ${DATABASE}.STORED_PROCEDURES_MART`,

    // Create a table to store warehouse metering history and grant ownership and select permissions.
    `CREATE OR REPLACE TABLE 
    ${DATABASE}.STORED_PROCEDURES_MART.WAREHOUSE_METERING_HISTORY AS (
      SELECT * 
      FROM ${DATABASE}.STORED_PROCEDURES.WAREHOUSE_METERING_HISTORY
    );`,
    `GRANT OWNERSHIP ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.WAREHOUSE_METERING_HISTORY 
      TO ROLE ${DATABASE}_WRITE COPY CURRENT GRANTS;`,
    `GRANT SELECT ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.WAREHOUSE_METERING_HISTORY 
      TO ROLE ${DATABASE}_READ;`,


    // Create a table to store snowpipes and grant ownership and select permissions.
    `CREATE OR REPLACE TABLE ${DATABASE}.STORED_PROCEDURES_MART.SNOWPIPES AS (
      SELECT 
        *, 
        (SELECT MAX(INGESTION_TIME) FROM ${DATABASE}.STORED_PROCEDURES.SNOWPIPES) AS LATEST_INGESTION_TIME, 
        INGESTION_TIME = LATEST_INGESTION_TIME AS IS_LATEST 
      FROM ${DATABASE}.STORED_PROCEDURES.SNOWPIPES
    );`,
    `GRANT OWNERSHIP ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.SNOWPIPES 
      TO ROLE ${DATABASE}_WRITE COPY CURRENT GRANTS;`,
    `GRANT SELECT ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.SNOWPIPES 
      TO ROLE ${DATABASE}_READ;`,


    // Create a table to store snowpipe usage history and grant ownership and select permissions.
    `CREATE OR REPLACE TABLE ${DATABASE}.STORED_PROCEDURES_MART.SNOWPIPE_USAGE_HISTORY AS (
      SELECT * 
      FROM ${DATABASE}.STORED_PROCEDURES.SNOWPIPE_USAGE_HISTORY
    );`,
    `GRANT OWNERSHIP ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.SNOWPIPE_USAGE_HISTORY 
      TO ROLE ${DATABASE}_WRITE COPY CURRENT GRANTS;`,
    `GRANT SELECT ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.SNOWPIPE_USAGE_HISTORY 
      TO ROLE ${DATABASE}_READ;`,


    // Create a table to store query history and grant ownership and select permissions.
    `CREATE OR REPLACE TABLE ${DATABASE}.STORED_PROCEDURES_MART.QUERY_HISTORY AS (
      SELECT * 
      FROM ${DATABASE}.STORED_PROCEDURES.QUERY_HISTORY
    );`,
    `GRANT OWNERSHIP ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.QUERY_HISTORY 
      TO ROLE ${DATABASE}_WRITE COPY CURRENT GRANTS;`,
    `GRANT SELECT ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.QUERY_HISTORY 
      TO ROLE ${DATABASE}_READ;`,
    
    // Create a table to store tasks and grant ownership and select permissions.
    `CREATE OR REPLACE TABLE ${DATABASE}.STORED_PROCEDURES_MART.TASKS AS (
      SELECT *, 
        (SELECT MAX(INGESTION_TIME) FROM ${DATABASE}.STORED_PROCEDURES.TASK_VERSIONS) AS LATEST_INGESTION_TIME, 
        INGESTION_TIME = LATEST_INGESTION_TIME AS IS_LATEST 
      FROM ${DATABASE}.STORED_PROCEDURES.TASK_VERSIONS
    );`,
    `GRANT OWNERSHIP ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.TASKS 
      TO ROLE ${DATABASE}_WRITE COPY CURRENT GRANTS;`,
    `GRANT SELECT ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.TASKS TO ROLE ${DATABASE}_READ;`,
    
    // Create a table to store task usage history and grant ownership and select permissions. 
    `CREATE OR REPLACE TABLE 
    ${DATABASE}.STORED_PROCEDURES_MART.TASK_USAGE_HISTORY AS ( 
      SELECT * 
      FROM ${DATABASE}.STORED_PROCEDURES.TASK_HISTORY 
    );`, 
    `GRANT OWNERSHIP ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.TASK_USAGE_HISTORY TO ROLE ${DATABASE}_WRITE COPY CURRENT GRANTS;`,
    `GRANT SELECT ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.TASK_USAGE_HISTORY TO ROLE ${DATABASE}_READ;`, 
    
    //Create a table to store snowflake compute costs and grant ownership and select permissions. 
    `CREATE OR REPLACE TABLE 
    ${DATABASE}.STORED_PROCEDURES_MART.SNOWFLAKE_COST AS ( 
      SELECT 
        SUM(CREDITS_USED) * 3.00 AS COST, 
        START_TIME::DATE AS START_DATE 
      FROM ${DATABASE}.STORED_PROCEDURES_MART.WAREHOUSE_METERING_HISTORY 
      GROUP BY START_DATE
    );`, 
    `GRANT OWNERSHIP ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.SNOWFLAKE_COST TO ROLE ${DATABASE}_WRITE COPY CURRENT GRANTS;`, 
    `GRANT SELECT ON TABLE ${DATABASE}.STORED_PROCEDURES_MART.SNOWFLAKE_COST TO ROLE ${DATABASE}_READ;`
  ];

  
  var currCommand = 'not set yet';
  try { 
    if (DRY_RUN) { 
      return sqlCommands.join('\n'); 

    } else {
      sqlCommands.forEach((sqlCommand) => { 
        currCommand = sqlCommand;
        snowflake.execute({ sqlText: sqlCommand }); 
      });
    } 
    
  } catch (err)  {
    return  `
      Procedure Failed. 
        Message: ${err.message}

        Last SQL Command: ${currCommand}

        Stack Trace:
        ${err.stack}
    `;
  }
    
  return 'Successfully created mart tables';
$$;