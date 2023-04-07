-- call this procedure as securityadmin

CREATE OR REPLACE PROCEDURE 
RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCEDURES.DROP_SECURITY_OBJECTS(
    PROJECT_NAME STRING,
    DRY_RUN      BOOLEAN
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  const sqlCommands = [
    //=============================================================================
    // drop object access (OA) roles
    //=============================================================================
    // data access
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_RAW_READ;`,
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_RAW_OWNER;`,
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_DEV_READ;`,
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_DEV_WRITE;`,
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_TEST_READ;`,
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_TEST_WRITE;`,
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_PROD_READ;`,
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_PROD_WRITE;`,

    // warehouse access
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_DEV_WH_ALL_PRIVILEGES;`,
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_TEST_WH_USAGE;`,
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_PROD_WH_USAGE;`,
    //=============================================================================


    //=============================================================================
    // drop business function roles
    //=============================================================================
    // BF roles
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_ADMIN;`,
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_DEVELOPER;`,
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_DBT_TEST_SERVICE_ACCOUNT_ROLE;`,
    `DROP ROLE IF EXISTS ${PROJECT_NAME}_DBT_PROD_SERVICE_ACCOUNT_ROLE;`,
    //=============================================================================


    //=============================================================================
    // drop service accounts
    //=============================================================================
    // test service account
    `DROP USER IF EXISTS ${PROJECT_NAME}_DBT_TEST_SERVICE_ACCOUNT_USER;`,

    // prod service account
    `DROP USER IF EXISTS ${PROJECT_NAME}_DBT_PROD_SERVICE_ACCOUNT_USER;`,
    //=============================================================================
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
    
  return 'Successfully dropped security objects.';
$$;
