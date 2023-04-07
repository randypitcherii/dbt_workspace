{# goodnight sweet prince #} 
{% macro teardown_stored_procedures_demo(project_name='sp_to_dbt', dry_run=True) %}
    {% set sql %}
        USE ROLE SYSADMIN;

        // Databases
        DROP DATABASE IF EXISTS {{ project_name }}_RAW;
        DROP DATABASE IF EXISTS {{ project_name }}_DEV;
        DROP DATABASE IF EXISTS {{ project_name }}_TEST;
        DROP DATABASE IF EXISTS {{ project_name }}_PROD;

        // Warehouses
        DROP WAREHOUSE IF EXISTS {{ project_name }}_DEV_WH;
        DROP WAREHOUSE IF EXISTS {{ project_name }}_TEST_WH;
        DROP WAREHOUSE IF EXISTS {{ project_name }}_PROD_WH;


        USE ROLE SECURITYADMIN;
        // data access
        DROP ROLE IF EXISTS {{ project_name }}_RAW_READ;
        DROP ROLE IF EXISTS {{ project_name }}_RAW_OWNER;
        DROP ROLE IF EXISTS {{ project_name }}_DEV_READ;
        DROP ROLE IF EXISTS {{ project_name }}_DEV_WRITE;
        DROP ROLE IF EXISTS {{ project_name }}_TEST_READ;
        DROP ROLE IF EXISTS {{ project_name }}_TEST_WRITE;
        DROP ROLE IF EXISTS {{ project_name }}_PROD_READ;
        DROP ROLE IF EXISTS {{ project_name }}_PROD_WRITE;
        DROP ROLE IF EXISTS {{ project_name }}_OTHER_RAW_DATA_READ;

        // warehouse access
        DROP ROLE IF EXISTS {{ project_name }}_DEV_WH_ALL_PRIVILEGES;
        DROP ROLE IF EXISTS {{ project_name }}_TEST_WH_USAGE;
        DROP ROLE IF EXISTS {{ project_name }}_PROD_WH_USAGE;

        // BF roles
        DROP ROLE IF EXISTS {{ project_name }}_ADMIN;
        DROP ROLE IF EXISTS {{ project_name }}_DEVELOPER;
        DROP ROLE IF EXISTS {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_ROLE;
        DROP ROLE IF EXISTS {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_ROLE;

        // Service Accounts
        DROP USER IF EXISTS {{ project_name }}_DBT_TEST_SERVICE_ACCOUNT_USER;
        DROP USER IF EXISTS {{ project_name }}_DBT_PROD_SERVICE_ACCOUNT_USER;
    {% endset %}

    {% if dry_run %}
        {% do log(sql, False) %}
    {% else %}
        {% do run_query(sql) %}
    {% endif %}

    {{ return(sql) }}
{% endmacro %}