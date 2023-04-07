-- call this procedure as sysadmin

CREATE OR REPLACE PROCEDURE 
RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCEDURES.CREATE_TOP_LEVEL_OBJECTS(
    PROJECT_NAME VARCHAR, 
    DRY_RUN      BOOLEAN
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
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
    if (DRY_RUN) {
      return commands.join('\n');
    } else {
      commands.forEach((command) => {
        currCommand = command;
        snowflake.execute({sqlText: command})
      });
    }

  } catch (err) {
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
