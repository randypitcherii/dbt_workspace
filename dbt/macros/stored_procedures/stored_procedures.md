
# Problems
- inconsistent style from SP to SP
- manual ordering
- no concurrency without advanced effort
- painful to change or update. You must understand the whole pipeline to safely make a change
- difficult to debug (errors in SP03 may be the result of code in SP01)
- failures block every subsequent command. 
- too many hard-coded references. Difficult to ensure dev/ci/prod environment isolation.
- absolutely no data quality testing
- often little or no version control history. How old is this code? Does it still work? Who owns it? Why do we do this???

# Approach
Split your stored procedures into 3 major categories:
- DDL - Object creation (Top Level vs Nested)
- DML - Data Processing (Initial Load vs Incremental)
- Housekeeping (permission updates, cleanup tasks, testing if any)

## DDL
- Top level object DDL become dbt Macros
- Tabular Nested objects (schemas, tables, views - aka relations) DDL is natively built by dbt
- Non-tabular nested objects (stages, stored procedures, user-defined functions, tasks) DDL become dbt Macros initially but better to use custom materializations (either from you or from a dbt Package).

## DML
- All DML becomes sql select statements as dbt Models
- Initial load logic is the easiest to migrate - just copy/paste and replace hard references with refs
- Incremental logic should be added to the initial load as an incremental dbt Model (materialization='incremental')

## Housekeeping
- Whenever possible, use built-in dbt support for common housekeeping tasks
- Leverage hooks (before/after total build or before/after individual object build)
- most housekeeping can be reduced through the use of jinja. Don't Repeat Yourself = DRY code