{{ config(materialized='copy_into') }}

select
     $1::string as show_id ,
     $2::string as show_idd ,
    current_timestamp::timestamp_ntz as ingestion_time

from
    @{{source('netflix', 'netflix_blob_stage')}}
