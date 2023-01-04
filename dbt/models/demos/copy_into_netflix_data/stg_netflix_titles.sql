{{ config(materialized='copy_into') }}

select 
    {% for x in range(1,30) %}
        ${{x}} as col_{{x}},
    {% endfor %}
    current_timestamp::timestamp_ntz as ingestion_time

from
  {{source('netflix', 'netflix_blob_stage')}} 
