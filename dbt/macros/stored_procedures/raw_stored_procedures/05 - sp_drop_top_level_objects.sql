-- Call this procedure as sysadmin

CREATE OR REPLACE PROCEDURE 
RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCEDURES.DROP_TOP_LEVEL_OBJECTS(
  PROJECT_NAME VARCHAR, 
  DRY_RUN BOOLEAN
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try {
    var commands = [];
    
    // Databases
    commands.push(`DROP DATABASE IF EXISTS ${PROJECT_NAME}_RAW;`);
    commands.push(`DROP DATABASE IF EXISTS ${PROJECT_NAME}_DEV;`);
    commands.push(`DROP DATABASE IF EXISTS ${PROJECT_NAME}_TEST;`);
    commands.push(`DROP DATABASE IF EXISTS ${PROJECT_NAME}_PROD;`);
    
    // dev warehouse
    commands.push(`DROP WAREHOUSE IF EXISTS ${PROJECT_NAME}_DEV_WH;`);
    
    // test warehouse
    commands.push(`DROP WAREHOUSE IF EXISTS ${PROJECT_NAME}_TEST_WH;`);
    
    // prod warehouse
    commands.push(`DROP WAREHOUSE IF EXISTS ${PROJECT_NAME}_PROD_WH;`);
    

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

  } catch (err)  {
    return `
      Procedure Failed. 
        Message: ${err.message}

        currCommand: ${currCommand}

        Stack Trace:
        ${err.stack}
    `;
  }

  return 'Successfully dropped top level objects';
$$;
