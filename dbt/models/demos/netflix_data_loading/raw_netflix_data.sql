{{ config(materialized='easy_incremental', watermark='date_added') }}

{% set target_stage='randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage'%}

with 

raw_data as (
    select
        {{ get_stage_columns_from_loading_config(
            stage_name=target_stage, 
            config_table=ref('loading_config')
        )}},
        current_timestamp::timestamp_ntz as ingestion_time
    
    from
        @{{target_stage}}
)


select * from raw_data