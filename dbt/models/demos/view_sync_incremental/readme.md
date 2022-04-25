### Purpose

This folder provides an example of using a [custom materialization](https://docs.getdbt.com/docs/guides/creating-new-materializations) to templatize DDL behavior in dbt.

This example does the following:

* Adds a `view_sync_incremental` materialization with custom logic
* Creates the source SQL of the incremental model as a `VIEW` instead of a temp table. Some dbt users do no have freedom to create tables in their target schemas  
* Based on the `unique_key` specified in the model's configuration, deletes records from the target table that do not exist in the source table 
