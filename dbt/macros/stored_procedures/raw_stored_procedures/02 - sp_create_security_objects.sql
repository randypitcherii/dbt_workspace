-- call this procedure as securityadmin

CREATE OR REPLACE PROCEDURE 
RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCEDURES.CREATE_SECURITY_OBJECTS(
    PROJECT_NAME            STRING,
    TEST_SVC_ACCNT_PASSWORD STRING,
    PROD_SVC_ACCNT_PASSWORD STRING,
    DRY_RUN                 BOOLEAN
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  const sqlCommands = [
    //=============================================================================
    // create object access (OA) roles
    //=============================================================================
    // data access
    `CREATE ROLE ${PROJECT_NAME}_RAW_READ;`,
    `CREATE ROLE ${PROJECT_NAME}_RAW_OWNER;`,
    `CREATE ROLE ${PROJECT_NAME}_DEV_READ;`,
    `CREATE ROLE ${PROJECT_NAME}_DEV_WRITE;`,
    `CREATE ROLE ${PROJECT_NAME}_TEST_READ;`,
    `CREATE ROLE ${PROJECT_NAME}_TEST_WRITE;`,
    `CREATE ROLE ${PROJECT_NAME}_PROD_READ;`,
    `CREATE ROLE ${PROJECT_NAME}_PROD_WRITE;`,

    // warehouse access
    `CREATE ROLE ${PROJECT_NAME}_DEV_WH_ALL_PRIVILEGES;`,
    `CREATE ROLE ${PROJECT_NAME}_TEST_WH_USAGE;`,
    `CREATE ROLE ${PROJECT_NAME}_PROD_WH_USAGE;`,


    // grant all roles to sysadmin (always do this)
    `GRANT ROLE ${PROJECT_NAME}_RAW_OWNER             TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_RAW_READ              TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_DEV_WRITE             TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_DEV_READ              TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_TEST_WRITE            TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_TEST_READ             TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_PROD_WRITE            TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_PROD_READ             TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_DEV_WH_ALL_PRIVILEGES TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_TEST_WH_USAGE         TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_PROD_WH_USAGE         TO ROLE SYSADMIN;`,
    //=============================================================================


    //=============================================================================
    // grant privileges to object access roles
    //=============================================================================
    // raw data access
    `GRANT OWNERSHIP ON DATABASE ${PROJECT_NAME}_RAW                           TO ROLE ${PROJECT_NAME}_RAW_OWNER;`,
    `GRANT USAGE ON DATABASE ${PROJECT_NAME}_RAW                               TO ROLE ${PROJECT_NAME}_RAW_READ;`,
    `GRANT USAGE ON ALL SCHEMAS IN DATABASE ${PROJECT_NAME}_RAW                TO ROLE ${PROJECT_NAME}_RAW_READ;`,
    `GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${PROJECT_NAME}_RAW             TO ROLE ${PROJECT_NAME}_RAW_READ;`,
    `GRANT SELECT ON ALL TABLES IN DATABASE ${PROJECT_NAME}_RAW                TO ROLE ${PROJECT_NAME}_RAW_READ;`,
    `GRANT SELECT ON ALL VIEWS IN DATABASE ${PROJECT_NAME}_RAW                 TO ROLE ${PROJECT_NAME}_RAW_READ;`,
    `GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE ${PROJECT_NAME}_RAW    TO ROLE ${PROJECT_NAME}_RAW_READ;`,
    `GRANT SELECT ON FUTURE TABLES IN DATABASE ${PROJECT_NAME}_RAW             TO ROLE ${PROJECT_NAME}_RAW_READ;`,
    `GRANT SELECT ON FUTURE VIEWS IN DATABASE ${PROJECT_NAME}_RAW              TO ROLE ${PROJECT_NAME}_RAW_READ;`,
    `GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE ${PROJECT_NAME}_RAW TO ROLE ${PROJECT_NAME}_RAW_READ;`,

    // dev data access
    `GRANT USAGE ON DATABASE ${PROJECT_NAME}_DEV                               TO ROLE ${PROJECT_NAME}_DEV_READ;`,
    `GRANT USAGE ON ALL SCHEMAS IN DATABASE ${PROJECT_NAME}_DEV                TO ROLE ${PROJECT_NAME}_DEV_READ;`,
    `GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${PROJECT_NAME}_DEV             TO ROLE ${PROJECT_NAME}_DEV_READ;`,
    `GRANT SELECT ON ALL TABLES IN DATABASE ${PROJECT_NAME}_DEV                TO ROLE ${PROJECT_NAME}_DEV_READ;`,
    `GRANT SELECT ON ALL VIEWS IN DATABASE ${PROJECT_NAME}_DEV                 TO ROLE ${PROJECT_NAME}_DEV_READ;`,
    `GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE ${PROJECT_NAME}_DEV    TO ROLE ${PROJECT_NAME}_DEV_READ;`,
    `GRANT SELECT ON FUTURE TABLES IN DATABASE ${PROJECT_NAME}_DEV             TO ROLE ${PROJECT_NAME}_DEV_READ;`,
    `GRANT SELECT ON FUTURE VIEWS IN DATABASE ${PROJECT_NAME}_DEV              TO ROLE ${PROJECT_NAME}_DEV_READ;`,
    `GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE ${PROJECT_NAME}_DEV TO ROLE ${PROJECT_NAME}_DEV_READ;`,
    `GRANT ROLE ${PROJECT_NAME}_DEV_READ                                       TO ROLE ${PROJECT_NAME}_DEV_WRITE;`,
    `GRANT CREATE SCHEMA ON DATABASE ${PROJECT_NAME}_DEV                       TO ROLE ${PROJECT_NAME}_DEV_WRITE;`,

    // test data access
    `GRANT USAGE ON DATABASE ${PROJECT_NAME}_TEST                               TO ROLE ${PROJECT_NAME}_TEST_READ;`,
    `GRANT USAGE ON ALL SCHEMAS IN DATABASE ${PROJECT_NAME}_TEST                TO ROLE ${PROJECT_NAME}_TEST_READ;`,
    `GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${PROJECT_NAME}_TEST             TO ROLE ${PROJECT_NAME}_TEST_READ;`,
    `GRANT SELECT ON ALL TABLES IN DATABASE ${PROJECT_NAME}_TEST                TO ROLE ${PROJECT_NAME}_TEST_READ;`,
    `GRANT SELECT ON ALL VIEWS IN DATABASE ${PROJECT_NAME}_TEST                 TO ROLE ${PROJECT_NAME}_TEST_READ;`,
    `GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE ${PROJECT_NAME}_TEST    TO ROLE ${PROJECT_NAME}_TEST_READ;`,
    `GRANT SELECT ON FUTURE TABLES IN DATABASE ${PROJECT_NAME}_TEST             TO ROLE ${PROJECT_NAME}_TEST_READ;`,
    `GRANT SELECT ON FUTURE VIEWS IN DATABASE ${PROJECT_NAME}_TEST              TO ROLE ${PROJECT_NAME}_TEST_READ;`,
    `GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE ${PROJECT_NAME}_TEST TO ROLE ${PROJECT_NAME}_TEST_READ;`,
    `GRANT ROLE ${PROJECT_NAME}_TEST_READ                                       TO ROLE ${PROJECT_NAME}_TEST_WRITE;`,
    `GRANT CREATE SCHEMA ON DATABASE ${PROJECT_NAME}_TEST                       TO ROLE ${PROJECT_NAME}_TEST_WRITE;`,

    // prod data access
    `GRANT USAGE ON DATABASE ${PROJECT_NAME}_PROD                               TO ROLE ${PROJECT_NAME}_PROD_READ;`,
    `GRANT USAGE ON ALL SCHEMAS IN DATABASE ${PROJECT_NAME}_PROD                TO ROLE ${PROJECT_NAME}_PROD_READ;`,
    `GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${PROJECT_NAME}_PROD             TO ROLE ${PROJECT_NAME}_PROD_READ;`,
    `GRANT SELECT ON ALL TABLES IN DATABASE ${PROJECT_NAME}_PROD                TO ROLE ${PROJECT_NAME}_PROD_READ;`,
    `GRANT SELECT ON ALL VIEWS IN DATABASE ${PROJECT_NAME}_PROD                 TO ROLE ${PROJECT_NAME}_PROD_READ;`,
    `GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE ${PROJECT_NAME}_PROD    TO ROLE ${PROJECT_NAME}_PROD_READ;`,
    `GRANT SELECT ON FUTURE TABLES IN DATABASE ${PROJECT_NAME}_PROD             TO ROLE ${PROJECT_NAME}_PROD_READ;`,
    `GRANT SELECT ON FUTURE VIEWS IN DATABASE ${PROJECT_NAME}_PROD              TO ROLE ${PROJECT_NAME}_PROD_READ;`,
    `GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE ${PROJECT_NAME}_PROD TO ROLE ${PROJECT_NAME}_PROD_READ;`,
    `GRANT ROLE ${PROJECT_NAME}_PROD_READ                                       TO ROLE ${PROJECT_NAME}_PROD_WRITE;`,
    `GRANT CREATE SCHEMA ON DATABASE ${PROJECT_NAME}_PROD                       TO ROLE ${PROJECT_NAME}_PROD_WRITE;`,

    // warehouse access
    `GRANT ALL PRIVILEGES ON WAREHOUSE ${PROJECT_NAME}_DEV_WH TO ROLE ${PROJECT_NAME}_DEV_WH_ALL_PRIVILEGES;`,
    `GRANT USAGE ON WAREHOUSE ${PROJECT_NAME}_TEST_WH         TO ROLE ${PROJECT_NAME}_TEST_WH_USAGE;`,
    `GRANT USAGE ON WAREHOUSE ${PROJECT_NAME}_PROD_WH         TO ROLE ${PROJECT_NAME}_PROD_WH_USAGE;`,
    //=============================================================================


    //=============================================================================
    // create business function roles and grant access to object access roles
    //=============================================================================
    // BF roles
    `CREATE ROLE ${PROJECT_NAME}_ADMIN;`,
    `CREATE ROLE ${PROJECT_NAME}_DEVELOPER;`,
    `CREATE ROLE ${PROJECT_NAME}_DBT_TEST_SERVICE_ACCOUNT_ROLE;`,
    `CREATE ROLE ${PROJECT_NAME}_DBT_PROD_SERVICE_ACCOUNT_ROLE;`,

    // grant all roles to sysadmin (always do this)
    `GRANT ROLE ${PROJECT_NAME}_ADMIN                         TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_DEVELOPER                     TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_DBT_TEST_SERVICE_ACCOUNT_ROLE TO ROLE SYSADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_DBT_PROD_SERVICE_ACCOUNT_ROLE TO ROLE SYSADMIN;`,

    // grant bf roles to admin
    `GRANT ROLE ${PROJECT_NAME}_RAW_OWNER                     TO ROLE ${PROJECT_NAME}_ADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_DEVELOPER                     TO ROLE ${PROJECT_NAME}_ADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_DBT_TEST_SERVICE_ACCOUNT_ROLE TO ROLE ${PROJECT_NAME}_ADMIN;`,
    `GRANT ROLE ${PROJECT_NAME}_DBT_PROD_SERVICE_ACCOUNT_ROLE TO ROLE ${PROJECT_NAME}_ADMIN;`,

    // grant OA roles to the developer
    `GRANT ROLE ${PROJECT_NAME}_RAW_READ                      TO ROLE ${PROJECT_NAME}_DEVELOPER;`,
    `GRANT ROLE ${PROJECT_NAME}_DEV_WRITE                     TO ROLE ${PROJECT_NAME}_DEVELOPER;`,
    `GRANT ROLE ${PROJECT_NAME}_TEST_READ                     TO ROLE ${PROJECT_NAME}_DEVELOPER;`,
    `GRANT ROLE ${PROJECT_NAME}_PROD_READ                     TO ROLE ${PROJECT_NAME}_DEVELOPER;`,
    `GRANT ROLE ${PROJECT_NAME}_DEV_WH_ALL_PRIVILEGES         TO ROLE ${PROJECT_NAME}_DEVELOPER;`,

    // grant OA roles to the test service account role
    `GRANT ROLE ${PROJECT_NAME}_TEST_WRITE          TO ROLE ${PROJECT_NAME}_DBT_TEST_SERVICE_ACCOUNT_ROLE;`,
    `GRANT ROLE ${PROJECT_NAME}_RAW_READ            TO ROLE ${PROJECT_NAME}_DBT_TEST_SERVICE_ACCOUNT_ROLE;`,
    `GRANT ROLE ${PROJECT_NAME}_TEST_WH_USAGE       TO ROLE ${PROJECT_NAME}_DBT_TEST_SERVICE_ACCOUNT_ROLE;`,

    // grant OA roles to the prod service account role
    `GRANT ROLE ${PROJECT_NAME}_PROD_WRITE          TO ROLE ${PROJECT_NAME}_DBT_PROD_SERVICE_ACCOUNT_ROLE;`,
    `GRANT ROLE ${PROJECT_NAME}_RAW_READ            TO ROLE ${PROJECT_NAME}_DBT_PROD_SERVICE_ACCOUNT_ROLE;`,
    `GRANT ROLE ${PROJECT_NAME}_PROD_WH_USAGE       TO ROLE ${PROJECT_NAME}_DBT_PROD_SERVICE_ACCOUNT_ROLE;`,
    //=============================================================================


    //=============================================================================
    // create service accounts
    //=============================================================================
    // test service account
    `CREATE USER ${PROJECT_NAME}_DBT_TEST_SERVICE_ACCOUNT_USER
        PASSWORD = '${TEST_SVC_ACCNT_PASSWORD}'
        COMMENT = 'Service account for dbt CI/CD in the test (TEST) environment of the ${PROJECT_NAME} project.'
        DEFAULT_WAREHOUSE = ${PROJECT_NAME}_TEST_WH
        DEFAULT_ROLE = ${PROJECT_NAME}_DBT_TEST_SERVICE_ACCOUNT_ROLE
        MUST_CHANGE_PASSWORD = FALSE;`,

    // prod service account
    `CREATE USER ${PROJECT_NAME}_DBT_PROD_SERVICE_ACCOUNT_USER
        PASSWORD = '${PROD_SVC_ACCNT_PASSWORD}'
        COMMENT = 'Service account for dbt CI/CD in the production (PROD) environment of the ${PROJECT_NAME} project.'
        DEFAULT_WAREHOUSE = ${PROJECT_NAME}_PROD_WH
        DEFAULT_ROLE = ${PROJECT_NAME}_DBT_PROD_SERVICE_ACCOUNT_ROLE
        MUST_CHANGE_PASSWORD = FALSE;`,

    // grant permissions to service accounts
    `GRANT ROLE ${PROJECT_NAME}_DBT_TEST_SERVICE_ACCOUNT_ROLE TO USER ${PROJECT_NAME}_DBT_TEST_SERVICE_ACCOUNT_USER;`,
    `GRANT ROLE ${PROJECT_NAME}_DBT_PROD_SERVICE_ACCOUNT_ROLE TO USER ${PROJECT_NAME}_DBT_PROD_SERVICE_ACCOUNT_USER;`,
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
    
  return 'Successfully created security objects';
$$;
