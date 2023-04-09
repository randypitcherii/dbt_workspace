{#    
    Run the following dbt command to generate the proper snowflake setup script. This does NOT execute anything in snowflake:
      dbt run-operation get_workspace_setup_script --args '{project_name: my_workspace, test_svc_accnt_password: <pwd_a>, prod_svc_accnt_password: <pwd_b>}'
    
    Also, use good, unique passwords. Or else... 
#}
{% macro 
  get_workspace_setup_script(
    project_name, 
    test_svc_accnt_password, 
    prod_svc_accnt_password
  ) 
%}

{% set setup_script %}

//=============================================================================
// create data resources
//=============================================================================
USE ROLE SYSADMIN;

// Databases
CREATE DATABASE {{ project_name }}_RAW;
CREATE DATABASE {{ project_name }}_DEV;
CREATE DATABASE {{ project_name }}_TEST;
CREATE DATABASE {{ project_name }}_PROD;

// Clean public schemas
DROP SCHEMA {{ project_name }}_RAW.PUBLIC;
DROP SCHEMA {{ project_name }}_DEV.PUBLIC;
DROP SCHEMA {{ project_name }}_TEST.PUBLIC;
DROP SCHEMA {{ project_name }}_PROD.PUBLIC;
//=============================================================================


//=============================================================================
// create warehouses
//=============================================================================
USE ROLE SYSADMIN;

// dev warehouse
CREATE WAREHOUSE {{ project_name }}_DEV_WH
  COMMENT='Warehouse for powering data engineering activities for the {{ project_name }} project'
  WAREHOUSE_SIZE=XSMALL
  AUTO_SUSPEND=60
  INITIALLY_SUSPENDED=TRUE;

// test warehouse
CREATE WAREHOUSE {{ project_name }}_TEST_WH
  COMMENT='Warehouse for powering test activities for the {{ project_name }} project'
  WAREHOUSE_SIZE=XSMALL
  AUTO_SUSPEND=60
  INITIALLY_SUSPENDED=TRUE;

// prod warehouse
CREATE WAREHOUSE {{ project_name }}_PROD_WH
  COMMENT='Warehouse for powering production activities for the {{ project_name }} project'
  WAREHOUSE_SIZE=XSMALL
  AUTO_SUSPEND=60
  INITIALLY_SUSPENDED=TRUE;
//=============================================================================


//=============================================================================
// create object access (OA) roles
//=============================================================================
USE ROLE SECURITYADMIN;

// data access
CREATE ROLE {{ project_name }}_RAW_READ;
CREATE ROLE {{ project_name }}_RAW_OWNER;
CREATE ROLE {{ project_name }}_DEV_READ;
CREATE ROLE {{ project_name }}_DEV_WRITE;
CREATE ROLE {{ project_name }}_TEST_READ;
CREATE ROLE {{ project_name }}_TEST_WRITE;
CREATE ROLE {{ project_name }}_PROD_READ;
CREATE ROLE {{ project_name }}_PROD_WRITE;
CREATE ROLE {{ project_name }}_OTHER_RAW_DATA_READ; -- Grant full source read access to this role. Think Fivetran read role for example.

// warehouse access
CREATE ROLE {{ project_name }}_DEV_WH_ALL_PRIVILEGES;
CREATE ROLE {{ project_name }}_TEST_WH_USAGE;
CREATE ROLE {{ project_name }}_PROD_WH_USAGE;

// grant all roles to sysadmin (always do this)
GRANT ROLE {{ project_name }}_RAW_OWNER             TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_RAW_READ              TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_DEV_WRITE             TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_DEV_READ              TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_TEST_WRITE            TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_TEST_READ             TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_PROD_WRITE            TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_PROD_READ             TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_OTHER_RAW_DATA_READ   TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_DEV_WH_ALL_PRIVILEGES TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_TEST_WH_USAGE         TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_PROD_WH_USAGE         TO ROLE SYSADMIN;
//=============================================================================


//=============================================================================
// grant privileges to object access roles
//=============================================================================
USE ROLE SECURITYADMIN;

// raw data access
GRANT OWNERSHIP ON DATABASE {{ project_name }}_RAW                           TO ROLE {{ project_name }}_RAW_OWNER;
GRANT USAGE ON DATABASE {{ project_name }}_RAW                               TO ROLE {{ project_name }}_RAW_READ;
GRANT USAGE ON ALL SCHEMAS IN DATABASE {{ project_name }}_RAW                TO ROLE {{ project_name }}_RAW_READ;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {{ project_name }}_RAW             TO ROLE {{ project_name }}_RAW_READ;
GRANT SELECT ON ALL TABLES IN DATABASE {{ project_name }}_RAW                TO ROLE {{ project_name }}_RAW_READ;
GRANT SELECT ON ALL VIEWS IN DATABASE {{ project_name }}_RAW                 TO ROLE {{ project_name }}_RAW_READ;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE {{ project_name }}_RAW    TO ROLE {{ project_name }}_RAW_READ;
GRANT SELECT ON FUTURE TABLES IN DATABASE {{ project_name }}_RAW             TO ROLE {{ project_name }}_RAW_READ;
GRANT SELECT ON FUTURE VIEWS IN DATABASE {{ project_name }}_RAW              TO ROLE {{ project_name }}_RAW_READ;
GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE {{ project_name }}_RAW TO ROLE {{ project_name }}_RAW_READ;

// dev data access
GRANT USAGE ON DATABASE {{ project_name }}_DEV                               TO ROLE {{ project_name }}_DEV_READ;
GRANT USAGE ON ALL SCHEMAS IN DATABASE {{ project_name }}_DEV                TO ROLE {{ project_name }}_DEV_READ;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {{ project_name }}_DEV             TO ROLE {{ project_name }}_DEV_READ;
GRANT SELECT ON ALL TABLES IN DATABASE {{ project_name }}_DEV                TO ROLE {{ project_name }}_DEV_READ;
GRANT SELECT ON ALL VIEWS IN DATABASE {{ project_name }}_DEV                 TO ROLE {{ project_name }}_DEV_READ;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE {{ project_name }}_DEV    TO ROLE {{ project_name }}_DEV_READ;
GRANT SELECT ON FUTURE TABLES IN DATABASE {{ project_name }}_DEV             TO ROLE {{ project_name }}_DEV_READ;
GRANT SELECT ON FUTURE VIEWS IN DATABASE {{ project_name }}_DEV              TO ROLE {{ project_name }}_DEV_READ;
GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE {{ project_name }}_DEV TO ROLE {{ project_name }}_DEV_READ;
GRANT ROLE {{ project_name }}_DEV_READ                                       TO ROLE {{ project_name }}_DEV_WRITE;
GRANT CREATE SCHEMA ON DATABASE {{ project_name }}_DEV                       TO ROLE {{ project_name }}_DEV_WRITE;

// test data access
GRANT USAGE ON DATABASE {{ project_name }}_TEST                               TO ROLE {{ project_name }}_TEST_READ;
GRANT USAGE ON ALL SCHEMAS IN DATABASE {{ project_name }}_TEST                TO ROLE {{ project_name }}_TEST_READ;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {{ project_name }}_TEST             TO ROLE {{ project_name }}_TEST_READ;
GRANT SELECT ON ALL TABLES IN DATABASE {{ project_name }}_TEST                TO ROLE {{ project_name }}_TEST_READ;
GRANT SELECT ON ALL VIEWS IN DATABASE {{ project_name }}_TEST                 TO ROLE {{ project_name }}_TEST_READ;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE {{ project_name }}_TEST    TO ROLE {{ project_name }}_TEST_READ;
GRANT SELECT ON FUTURE TABLES IN DATABASE {{ project_name }}_TEST             TO ROLE {{ project_name }}_TEST_READ;
GRANT SELECT ON FUTURE VIEWS IN DATABASE {{ project_name }}_TEST              TO ROLE {{ project_name }}_TEST_READ;
GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE {{ project_name }}_TEST TO ROLE {{ project_name }}_TEST_READ;
GRANT ROLE {{ project_name }}_TEST_READ                                       TO ROLE {{ project_name }}_TEST_WRITE;
GRANT CREATE SCHEMA ON DATABASE {{ project_name }}_TEST                       TO ROLE {{ project_name }}_TEST_WRITE;

// prod data access
GRANT USAGE ON DATABASE {{ project_name }}_PROD                               TO ROLE {{ project_name }}_PROD_READ;
GRANT USAGE ON ALL SCHEMAS IN DATABASE {{ project_name }}_PROD                TO ROLE {{ project_name }}_PROD_READ;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {{ project_name }}_PROD             TO ROLE {{ project_name }}_PROD_READ;
GRANT SELECT ON ALL TABLES IN DATABASE {{ project_name }}_PROD                TO ROLE {{ project_name }}_PROD_READ;
GRANT SELECT ON ALL VIEWS IN DATABASE {{ project_name }}_PROD                 TO ROLE {{ project_name }}_PROD_READ;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN DATABASE {{ project_name }}_PROD    TO ROLE {{ project_name }}_PROD_READ;
GRANT SELECT ON FUTURE TABLES IN DATABASE {{ project_name }}_PROD             TO ROLE {{ project_name }}_PROD_READ;
GRANT SELECT ON FUTURE VIEWS IN DATABASE {{ project_name }}_PROD              TO ROLE {{ project_name }}_PROD_READ;
GRANT SELECT ON FUTURE MATERIALIZED VIEWS IN DATABASE {{ project_name }}_PROD TO ROLE {{ project_name }}_PROD_READ;
GRANT ROLE {{ project_name }}_PROD_READ                                       TO ROLE {{ project_name }}_PROD_WRITE;
GRANT CREATE SCHEMA ON DATABASE {{ project_name }}_PROD                       TO ROLE {{ project_name }}_PROD_WRITE;

// warehouse access
GRANT ALL PRIVILEGES ON WAREHOUSE {{ project_name }}_DEV_WH TO ROLE {{ project_name }}_DEV_WH_ALL_PRIVILEGES;
GRANT USAGE ON WAREHOUSE {{ project_name }}_TEST_WH         TO ROLE {{ project_name }}_TEST_WH_USAGE;
GRANT USAGE ON WAREHOUSE {{ project_name }}_PROD_WH         TO ROLE {{ project_name }}_PROD_WH_USAGE;
//=============================================================================


//=============================================================================
// create business function roles and grant access to object access roles
//=============================================================================
USE ROLE SECURITYADMIN;

// BF roles
CREATE ROLE {{ project_name }}_ADMIN;
CREATE ROLE {{ project_name }}_DEVELOPER;
CREATE ROLE {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_ROLE;
CREATE ROLE {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_ROLE;

// grant all roles to sysadmin (always do this)
GRANT ROLE {{ project_name }}_ADMIN                         TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_DEVELOPER                     TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_ROLE TO ROLE SYSADMIN;
GRANT ROLE {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_ROLE TO ROLE SYSADMIN;

// grant bf roles to admin
GRANT ROLE {{ project_name }}_RAW_OWNER                     TO ROLE {{ project_name }}_ADMIN;
GRANT ROLE {{ project_name }}_DEVELOPER                     TO ROLE {{ project_name }}_ADMIN;
GRANT ROLE {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_ROLE TO ROLE {{ project_name }}_ADMIN;
GRANT ROLE {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_ROLE TO ROLE {{ project_name }}_ADMIN;

// grant OA roles to the developer
GRANT ROLE {{ project_name }}_RAW_READ                      TO ROLE {{ project_name }}_DEVELOPER;
GRANT ROLE {{ project_name }}_DEV_WRITE                     TO ROLE {{ project_name }}_DEVELOPER;
GRANT ROLE {{ project_name }}_TEST_READ                     TO ROLE {{ project_name }}_DEVELOPER;
GRANT ROLE {{ project_name }}_PROD_READ                     TO ROLE {{ project_name }}_DEVELOPER;
GRANT ROLE {{ project_name }}_OTHER_RAW_DATA_READ           TO ROLE {{ project_name }}_DEVELOPER;
GRANT ROLE {{ project_name }}_DEV_WH_ALL_PRIVILEGES         TO ROLE {{ project_name }}_DEVELOPER;

// grant OA roles to the test service account role
GRANT ROLE {{ project_name }}_TEST_WRITE          TO ROLE {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_ROLE;
GRANT ROLE {{ project_name }}_RAW_READ            TO ROLE {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_ROLE;
GRANT ROLE {{ project_name }}_OTHER_RAW_DATA_READ TO ROLE {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_ROLE;
GRANT ROLE {{ project_name }}_TEST_WH_USAGE       TO ROLE {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_ROLE;

// grant OA roles to the prod service account role
GRANT ROLE {{ project_name }}_PROD_WRITE          TO ROLE {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_ROLE;
GRANT ROLE {{ project_name }}_RAW_READ            TO ROLE {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_ROLE;
GRANT ROLE {{ project_name }}_OTHER_RAW_DATA_READ TO ROLE {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_ROLE;
GRANT ROLE {{ project_name }}_PROD_WH_USAGE       TO ROLE {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_ROLE;
//=============================================================================


//=============================================================================
// create service accounts
//=============================================================================
USE ROLE SECURITYADMIN;

// test service account
CREATE USER {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_USER
  PASSWORD = '{{ test_svc_accnt_password }}' 
  COMMENT = 'Service account for dbt CI/CD in the test (TEST) environment of the {{ project_name }} project.'
  DEFAULT_WAREHOUSE = {{ project_name }}_TEST_WH
  DEFAULT_ROLE = {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_ROLE
  MUST_CHANGE_PASSWORD = FALSE;

// prod service account
CREATE USER {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_USER
  PASSWORD = '{{ prod_svc_accnt_password }}' 
  COMMENT = 'Service account for dbt CI/CD in the production (PROD) environment of the {{ project_name }} project.'
  DEFAULT_WAREHOUSE = {{ project_name }}_PROD_WH
  DEFAULT_ROLE = {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_ROLE
  MUST_CHANGE_PASSWORD = FALSE;

// grant permissions to service accounts
GRANT ROLE {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_ROLE TO USER {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_USER;
GRANT ROLE {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_ROLE TO USER {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_USER;
//=============================================================================


//=============================================================================
// Connect this good funk to your existing warehouse junk
//=============================================================================
// Importantly, make sure you grant the proper read access for your sources to the {{ project_name }}_OTHER_RAW_DATA_READ role
// Something like:
USE ROLE SECURITYADMIN;
-- GRANT USAGE ON DATABASE FIVETRAN                   TO ROLE {{ project_name }}_OTHER_RAW_DATA_READ;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE FIVETRAN    TO ROLE {{ project_name }}_OTHER_RAW_DATA_READ;
-- GRANT USAGE ON FUTURE SCHEMAS IN DATABASE FIVETRAN TO ROLE {{ project_name }}_OTHER_RAW_DATA_READ;
-- GRANT SELECT ON ALL TABLES IN DATABASE FIVETRAN    TO ROLE {{ project_name }}_OTHER_RAW_DATA_READ;
-- GRANT SELECT ON FUTURE TABLES IN DATABASE FIVETRAN TO ROLE {{ project_name }}_OTHER_RAW_DATA_READ;

// Check out this example below for adding the snowflake account usage data to your dbt projects
USE ROLE SECURITYADMIN;
-- CREATE ROLE SNOWFLAKE_ACCOUNT_USAGE_READ_ROLE;
-- GRANT ROLE SNOWFLAKE_ACCOUNT_USAGE_READ_ROLE TO ROLE SYSADMIN; -- always do this
-- GRANT ROLE SNOWFLAKE_ACCOUNT_USAGE_READ_ROLE TO ROLE {{ project_name }}_OTHER_RAW_DATA_READ;

USE ROLE ACCOUNTADMIN;
-- GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE SNOWFLAKE_ACCOUNT_USAGE_READ_ROLE;

// Annnnd lastly, you should probably grant the dev permissions to some of your favorite people
USE ROLE SECURITYADMIN;
-- GRANT ROLE {{ project_name }}_ADMIN     TO USER LILY;
-- GRANT ROLE {{ project_name }}_DEVELOPER TO USER PENELOPE;
//=============================================================================
{% endset %}

{% do log('\n\n' ~ setup_script ~ '\n\n', True) %}
{{ return(setup_script) }}
{% endmacro %}