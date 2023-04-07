-- Run as SECURITYADMIN

CREATE OR REPLACE PROCEDURE create_project_roles_and_access(
    project_name STRING,
    test_svc_accnt_password STRING,
    prod_svc_accnt_password STRING,
    dry_run BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
  const roleStatements = [
    // data access
    `CREATE ROLE ${project_name}_RAW_READ;`,
    `CREATE ROLE ${project_name}_RAW_OWNER;`,
    `CREATE ROLE ${project_name}_DEV_READ;`,
    `CREATE ROLE ${project_name}_DEV_WRITE;`,
    `CREATE ROLE ${project_name}_TEST_READ;`,
    `CREATE ROLE ${project_name}_TEST_WRITE;`,
    `CREATE ROLE ${project_name}_PROD_READ;`,
    `CREATE ROLE ${project_name}_PROD_WRITE;`,
    `CREATE ROLE ${project_name}_DEV_STORED_PROCS_MART_READ;`,

    // warehouse access
    `CREATE ROLE ${project_name}_DEV_WH_ALL_PRIVILEGES;`,
    `CREATE ROLE ${project_name}_TEST_WH_USAGE;`,
    `CREATE ROLE ${project_name}_PROD_WH_USAGE;`,

    // grant all roles to sysadmin (always do this)
    `GRANT ROLE ${project_name}_RAW_OWNER                  TO ROLE SYSADMIN;`,
    `GRANT ROLE ${project_name}_RAW_READ                   TO ROLE SYSADMIN;`,
    `GRANT ROLE ${project_name}_DEV_WRITE                  TO ROLE SYSADMIN;`,
    `GRANT ROLE ${project_name}_DEV_READ                   TO ROLE SYSADMIN;`,
    `GRANT ROLE ${project_name}_TEST_WRITE                 TO ROLE SYSADMIN;`,
    `GRANT ROLE ${project_name}_TEST_READ                  TO ROLE SYSADMIN;`,
    `GRANT ROLE ${project_name}_PROD_WRITE                 TO ROLE SYSADMIN;`,
    `GRANT ROLE ${project_name}_PROD_READ                  TO ROLE SYSADMIN;`,
    `GRANT ROLE ${project_name}_DEV_STORED_PROCS_MART_READ TO ROLE SYSADMIN;`,
    `GRANT ROLE ${project_name}_DEV_WH_ALL_PRIVILEGES      TO ROLE SYSADMIN;`,
    `GRANT ROLE ${project_name}_TEST_WH_USAGE              TO ROLE SYSADMIN;`,
    `GRANT ROLE ${project_name}_PROD_WH_USAGE              TO ROLE SYSADMIN;`,

    // raw data access
    `GRANT OWNERSHIP ON DATABASE ${project_name}_RAW                           TO ROLE ${project_name}_RAW_OWNER;`,
    `GRANT USAGE ON DATABASE ${project_name}_RAW                               TO ROLE ${project_name}_RAW_READ;`,
    `GRANT USAGE ON ALL SCHEMAS IN DATABASE ${project_name}_RAW                TO ROLE ${project_name}_RAW_READ;`,
    `GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${project_name}_RAW             TO ROLE ${project_name}_RAW_READ;`,
    `GRANT SELECT ON ALL TABLES IN DATABASE ${project_name}_RAW                TO ROLE ${project_name}_RAW_READ;`,
    `GRANT SELECT ON ALL VIEWS IN DATABASE ${project_name}_RAW                 TO ROLE ${project_name}_RAW_READ;`,
    `GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE ${project_name}_RAW    TO ROLE ${project_name}_RAW_READ;`,
    `GRANT SELECT ON FUTURE TABLES IN DATABASE ${project_name}_RAW             TO ROLE ${project_name}_RAW_READ;`,
    `GRANT SELECT ON FUTURE VIEWS IN DATABASE ${project_name}_RAW              TO ROLE ${project_name}_RAW_READ;`,
    `GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE ${project_name}_RAW TO ROLE ${project_name}_RAW_READ;`,

    // dev data access
    `GRANT USAGE ON DATABASE ${project_name}_DEV                               TO ROLE ${project_name}_DEV_READ;`,
    `GRANT USAGE ON ALL SCHEMAS IN DATABASE ${project_name}_DEV                TO ROLE ${project_name}_DEV_READ;`,
    `GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${project_name}_DEV             TO ROLE ${project_name}_DEV_READ;`,
    `GRANT SELECT ON ALL TABLES IN DATABASE ${project_name}_DEV                TO ROLE ${project_name}_DEV_READ;`,
    `GRANT SELECT ON ALL VIEWS IN DATABASE ${project_name}_DEV                 TO ROLE ${project_name}_DEV_READ;`,
    `GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE ${project_name}_DEV    TO ROLE ${project_name}_DEV_READ;`,
    `GRANT SELECT ON FUTURE TABLES IN DATABASE ${project_name}_DEV             TO ROLE ${project_name}_DEV_READ;`,
    `GRANT SELECT ON FUTURE VIEWS IN DATABASE ${project_name}_DEV              TO ROLE ${project_name}_DEV_READ;`,
    `GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE ${project_name}_DEV TO ROLE ${project_name}_DEV_READ;`,
    `GRANT ROLE ${project_name}_DEV_READ                                       TO ROLE ${project_name}_DEV_WRITE;`,
    `GRANT CREATE SCHEMA ON DATABASE ${project_name}_DEV                       TO ROLE ${project_name}_DEV_WRITE;`,

    // test data access
    `GRANT USAGE ON DATABASE ${project_name}_TEST                               TO ROLE ${project_name}_TEST_READ;`,
    `GRANT USAGE ON ALL SCHEMAS IN DATABASE ${project_name}_TEST                TO ROLE ${project_name}_TEST_READ;`,
    `GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${project_name}_TEST             TO ROLE ${project_name}_TEST_READ;`,
    `GRANT SELECT ON ALL TABLES IN DATABASE ${project_name}_TEST                TO ROLE ${project_name}_TEST_READ;`,
    `GRANT SELECT ON ALL VIEWS IN DATABASE ${project_name}_TEST                 TO ROLE ${project_name}_TEST_READ;`,
    `GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE ${project_name}_TEST    TO ROLE ${project_name}_TEST_READ;`,
    `GRANT SELECT ON FUTURE TABLES IN DATABASE ${project_name}_TEST             TO ROLE ${project_name}_TEST_READ;`,
    `GRANT SELECT ON FUTURE VIEWS IN DATABASE ${project_name}_TEST              TO ROLE ${project_name}_TEST_READ;`,
    `GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE ${project_name}_TEST TO ROLE ${project_name}_TEST_READ;`,
    `GRANT ROLE ${project_name}_TEST_READ                                       TO ROLE ${project_name}_TEST_WRITE;`,
    `GRANT CREATE SCHEMA ON DATABASE ${project_name}_TEST                       TO ROLE ${project_name}_TEST_WRITE;`,

    // prod data access
    `GRANT USAGE ON DATABASE ${project_name}_PROD                               TO ROLE ${project_name}_PROD_READ;`,
    `GRANT USAGE ON ALL SCHEMAS IN DATABASE ${project_name}_PROD                TO ROLE ${project_name}_PROD_READ;`,
    `GRANT USAGE ON FUTURE SCHEMAS IN DATABASE ${project_name}_PROD             TO ROLE ${project_name}_PROD_READ;`,
    `GRANT SELECT ON ALL TABLES IN DATABASE ${project_name}_PROD                TO ROLE ${project_name}_PROD_READ;`,
    `GRANT SELECT ON ALL VIEWS IN DATABASE ${project_name}_PROD                 TO ROLE ${project_name}_PROD_READ;`,
    `GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE ${project_name}_PROD    TO ROLE ${project_name}_PROD_READ;`,
    `GRANT SELECT ON FUTURE TABLES IN DATABASE ${project_name}_PROD             TO ROLE ${project_name}_PROD_READ;`,
    `GRANT SELECT ON FUTURE VIEWS IN DATABASE ${project_name}_PROD              TO ROLE ${project_name}_PROD_READ;`,
    `GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE ${project_name}_PROD TO ROLE ${project_name}_PROD_READ;`,
    `GRANT ROLE ${project_name}_PROD_READ                                       TO ROLE ${project_name}_PROD_WRITE;`,
    `GRANT CREATE SCHEMA ON DATABASE ${project_name}_PROD                       TO ROLE ${project_name}_PROD_WRITE;`,

    // warehouse access
    `GRANT ALL PRIVILEGES ON WAREHOUSE ${project_name}_DEV_WH TO ROLE ${project_name}_DEV_WH_ALL_PRIVILEGES;`, 
    `GRANT USAGE ON WAREHOUSE ${project_name}_TEST_WH         TO ROLE ${project_name}_TEST_WH_USAGE;`, 
    `GRANT USAGE ON WAREHOUSE ${project_name}_PROD_WH         TO ROLE ${project_name}_PROD_WH_USAGE;`,
    
    // test and prod service account access
    `CREATE USER ${project_name}_TEST_SVC_ACCNT PASSWORD = "${test_svc_accnt_password}";`,
    `CREATE USER ${project_name}_PROD_SVC_ACCNT PASSWORD = "${prod_svc_accnt_password}";`,
    `GRANT ROLE ${project_name}_RAW_READ              TO USER ${project_name}_TEST_SVC_ACCNT;`,
    `GRANT ROLE ${project_name}_RAW_READ              TO USER ${project_name}_PROD_SVC_ACCNT;`,
    `GRANT ROLE ${project_name}_DEV_READ              TO USER ${project_name}_TEST_SVC_ACCNT;`,
    `GRANT ROLE ${project_name}_DEV_WRITE             TO USER ${project_name}_TEST_SVC_ACCNT;`,
    `GRANT ROLE ${project_name}_DEV_READ              TO USER ${project_name}_PROD_SVC_ACCNT;`,
    `GRANT ROLE ${project_name}_TEST_READ             TO USER ${project_name}_TEST_SVC_ACCNT;`,
    `GRANT ROLE ${project_name}_TEST_WRITE            TO USER ${project_name}_TEST_SVC_ACCNT;`,
    `GRANT ROLE ${project_name}_PROD_READ             TO USER ${project_name}_PROD_SVC_ACCNT;`,
    `GRANT ROLE ${project_name}_PROD_WRITE            TO USER ${project_name}_PROD_SVC_ACCNT;`,
    `GRANT ROLE ${project_name}_DEV_WH_ALL_PRIVILEGES TO USER ${project_name}_TEST_SVC_ACCNT;`,
    `GRANT ROLE ${project_name}_DEV_WH_ALL_PRIVILEGES TO USER ${project_name}_PROD_SVC_ACCNT;`,
    `GRANT ROLE ${project_name}_TEST_WH_USAGE         TO USER ${project_name}_TEST_SVC_ACCNT;`,
    `GRANT ROLE ${project_name}_PROD_WH_USAGE         TO USER ${project_name}_PROD_SVC_ACCNT;`
  ];
  
  let result = [];
  
  try { 
    roleStatements.forEach((statement) => { 
      if (dry_run) { 
        result.push(statement); 
      } else { 
        snowflake.execute({ sqlText: statement }); 
      } 
    }); 
    
    if (dry_run) { 
      return result; 
    } else { 
      return "Success"; 
    } 
  } catch (err)  {
    var result =  `
      Procedure Failed. 
        Code: ${err.code}
        State: ${err.state}
        Message: ${err.message}
        Stack Trace:
        ${err.stack}
    `;
    
    return result;
  }
$$;
