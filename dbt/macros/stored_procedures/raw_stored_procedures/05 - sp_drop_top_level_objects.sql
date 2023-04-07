USE ROLE SYSADMIN;

CREATE OR REPLACE PROCEDURE 
RANDY_PITCHER_WORKSPACE_DEV.STORED_PROCEDURES.DROP_DATA_RESOURCES(
  PROJECT_NAME VARCHAR, 
  DRY_RUN BOOLEAN
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
    commands.push(`DROP DATABASE ${PROJECT_NAME}_RAW;`);
    commands.push(`DROP DATABASE ${PROJECT_NAME}_DEV;`);
    commands.push(`DROP DATABASE ${PROJECT_NAME}_TEST;`);
    commands.push(`DROP DATABASE ${PROJECT_NAME}_PROD;`);
    
    // dev warehouse
    commands.push(`DROP WAREHOUSE ${PROJECT_NAME}_DEV_WH;`);
    
    // test warehouse
    commands.push(`DROP WAREHOUSE ${PROJECT_NAME}_TEST_WH;`);
    
    // prod warehouse
    commands.push(`DROP WAREHOUSE ${PROJECT_NAME}_PROD_WH;`);
    

    // execute
    var currCommand = 'not yet set';
    if (!DRY_RUN) {
      commands.forEach((command) => {
        currCommand = command;
        snowflake.execute({sqlText: command})
      });

      result = 'Data resources dropped.';

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
