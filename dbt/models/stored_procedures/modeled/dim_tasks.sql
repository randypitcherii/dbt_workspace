with 

lit as (
  select max(ingestion_time) as latest_ingestion_time
  from {{ ref('stg_task_versions') }}
)

select 
  *, 
  (select latest_ingestion_time from lit) as latest_ingestion_time,
  ingestion_time = latest_ingestion_time  as is_latest 

from {{ ref('stg_task_versions') }}
