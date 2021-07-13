{{ config(materialized='easy_incremental', watermark='date_added') }}

with 

raw_data as (
    select
        {{ get_stage_columns_from_loading_config(
            stage_name=target_stage, 
            config_table=ref('loading_config')
        )}},
        current_timestamp::timestamp_ntz as ingestion_time
    
    from
        @{{source('netflix', 'netflix_blob_stage')}}
)


select * from raw_data