{{ config(materialized='easy_incremental', watermark='date_added') }}

select
    {{ get_stage_columns_from_loading_config(
        stage_name=source('netflix', 'netflix_blob_stage'), 
        config_table=ref('loading_config')
    )}},
    current_timestamp::timestamp_ntz as ingestion_time

from
    @{{source('netflix', 'netflix_blob_stage')}}