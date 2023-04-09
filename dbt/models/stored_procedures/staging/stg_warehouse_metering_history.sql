select
    start_time,
    end_time,
    warehouse_id,
    warehouse_name,
    credits_used,
    credits_used_compute,
    credits_used_cloud_services,
    current_timestamp as ingestion_time
    
from {{ source('snowflake_account_usage', 'warehouse_metering_history') }}