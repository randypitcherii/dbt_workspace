-- using the copy into config i'm getting data from an externals stage and leveraging the "COPY INTO" command in snowflake
-- this will create a table that is the same name of your model and run "COPY INTO" every time you run only picking up new files
-- you can also use the "--full-refresh" flag to re-reun the create table statement before running "COPY INTO"
-- notice that i'm using the "@" before my source call to let Snowflake know it's a stage
-- I also provide a file format that I created in snowflake, in this file format I provide <DATABASE>.<SCHEMA>.<FILE_FORMAT_NAME>
-- File formats live inside a database & schema so you need these prefixes to pick up the right ones

{{ config(materialized='copy_into') }}

Select
    $1::NUMBER Price,
    $2::STRING as address,
    $3::STRING as Local_area,
    $4::STRING as zipcode,
    $5::NUMBER as beds,
    $6::NUMBER as baths,
    $7::NUMBER as sqft,
    $8::STRING as url,
    current_timestamp::timestamp_ntz as ingestion_time
From @{{source('external_adls_stage_database','azure_adls_external_stage')}}
(file_format => 'STEVE_D_RAW_DATA.EXTERNAL_STAGES_SAMPLES.basic_csv')