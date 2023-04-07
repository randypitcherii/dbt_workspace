USE ROLE SYSADMIN;

CREATE OR REPLACE PROCEDURE 
RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCEDURES.CREATE_DATA_RESOURCES(
  PROJECT_NAME VARCHAR, 
  DRY_RUN BOOLEAN
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  try {
    var result;
    var commands = [];
    
    // Databases
    commands.push(`CREATE DATABASE ${PROJECT_NAME}_RAW;`);
    commands.push(`CREATE DATABASE ${PROJECT_NAME}_DEV;`);
    commands.push(`CREATE DATABASE ${PROJECT_NAME}_TEST;`);
    commands.push(`CREATE DATABASE ${PROJECT_NAME}_PROD;`);
    
    // Clean public schemas
    commands.push(`DROP SCHEMA ${PROJECT_NAME}_RAW.PUBLIC;`);
    commands.push(`DROP SCHEMA ${PROJECT_NAME}_DEV.PUBLIC;`);
    commands.push(`DROP SCHEMA ${PROJECT_NAME}_TEST.PUBLIC;`);
    commands.push(`DROP SCHEMA ${PROJECT_NAME}_PROD.PUBLIC;`);
    
    // dev warehouse
    commands.push(`
      CREATE WAREHOUSE ${PROJECT_NAME}_DEV_WH 
        COMMENT='Warehouse for powering data engineering activities for the ${PROJECT_NAME} project'
        WAREHOUSE_SIZE=XSMALL 
        AUTO_SUSPEND=60 
        INITIALLY_SUSPENDED=TRUE;
    `);
    
    // test warehouse
    commands.push(`
      CREATE WAREHOUSE ${PROJECT_NAME}_TEST_WH 
        COMMENT='Warehouse for powering test activities for the ${PROJECT_NAME} project' WAREHOUSE_SIZE=XSMALL 
        AUTO_SUSPEND=60 
        INITIALLY_SUSPENDED=TRUE;
    `);
    
    // prod warehouse
    commands.push(`
      CREATE WAREHOUSE ${PROJECT_NAME}_PROD_WH 
        COMMENT='Warehouse for powering production activities for the ${PROJECT_NAME} project' 
        WAREHOUSE_SIZE=XSMALL 
        AUTO_SUSPEND=60 INITIALLY_SUSPENDED=TRUE;
    `);
    

    // execute
    var currCommand = 'not yet set';
    if (!DRY_RUN) {
      commands.forEach((command) => {
        currCommand = command;
        snowflake.execute({sqlText: command})
      });

      result = 'Data resources created.';

    } else {
      result = commands.join('\n');
    }

  } catch (err)  {
    result =  `
      Procedure Failed. 
        Message: ${err.message}
        currCommand: ${currCommand}
        Stack Trace:
        ${err.stack}
    `;
    
    return result;
  }

  return result;
$$;
