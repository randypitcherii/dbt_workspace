# Why?
The following problems are associated with data pipelines that rely on stored procedures:
- calling stored procedures require an active warehouse, even for administrative tasks that should not use warehouse credits.
- inconsistent style from procedure to procedure
- manual ordering of operations within and across stored procedures
- no concurrency without advanced effort
- painful to change or update. You must understand the whole pipeline to safely make a change
- difficult to debug (difficult to access logs, errors in SP03 may be the result of code in SP01)
- failures block every subsequent command. 
- too many hard-coded references. Difficult to ensure dev/ci/prod environment isolation.
- absolutely no data quality testing
- often little or no version control history. How old is this code? Does it still work? Who owns it? Why do we do this???
- limitations in stored procedures (can't switch roles, must understand caller/owner rights)
- Audit: Snowflake query history shows when you call a stored procedure, but not the contents of that procedure. This is a challenge, especially over time to see who ran what, when. 

Converting to dbt means:
- Less code
- Less complexity
- Less duplicate (or near duplicate) builds 
- More modularity
- More reusability
- More consistency
- More visibility
- Fewer mistakes
- Faster development
- Reliable deployments
- all while adding auto lineage, documentation, and data quality monitoring.
- Massive open source community (pool of hiring talent, most common problems have readily-available and proven solutions, high quality training and support options)
- more enjoyable development (this is subjective - if you disagree with this please reach out. I propose that no person fluent with dbt prefers writing stored procedures.)

This results in:
- Cheaper development (lower learning curve, faster onboarding, less rework, more people are fit to contribute, no need to hire all advanced warehouse users)
- Cheaper builds (consistency, concurrency, efficiency all lead to shorter runtimes)
- Reduced risk (timeline risk, data quality risk, developer adoption risk, consumer adoption risk)
- Reduced opportunity costs (what is the value of the analytics you don't have time for today?)
- Hedge against vendor lockin (dbt is open source and works with the top cloud data warehouses)
- Employee retention + easier, mroe reliable hiring

# How - Option 1: Lift and Shift Approach
- move your stored procedures into dbt macros. Just copy and paste.
- use `dbt run-operation your_macro_name` to execute your procedures as you typically would
- you can iterate on this by using dbt variables, more jinja, job orchestration + monitoring, and truly useful logging.

# How - Option 2: Modernization Approach
Split your stored procedures into 3 major categories:
- DDL - Object creation (Top Level vs Nested)
- DML - Data Processing (Initial Load vs Incremental)
- Housekeeping (permission updates, cleanup tasks, testing if any)

## DDL
- Top level object DDL become dbt Macros
- Tabular Nested objects (schemas, tables, views - aka relations) DDL is natively built by dbt
- Non-tabular nested objects (stages, stored procedures, user-defined functions, tasks) DDL become dbt Macros initially but better to use custom materializations (either from you or from a dbt Package).
- NOTE: `create table as` aka CTAS (sea-tazz) and similar syntax will be treated as DML here, so handle those in the DML section. 

## DML
- All DML becomes sql select statements as dbt Models
- Initial load logic is the easiest to migrate - just copy/paste and replace hard references with refs
- Incremental logic should be added to the initial load as an incremental dbt Model (materialization='incremental')

## Housekeeping
- Whenever possible, use built-in dbt support for common housekeeping tasks
- Leverage hooks (before/after total build or before/after individual object build)
- most housekeeping can be reduced through the use of jinja. Don't Repeat Yourself = DRY code

# Outcomes (metrics)
- Lines of code
- Runtime
- Simplicity of changes
- Simplicity of troubleshooting