{% snapshot snpsht__snowflake_query_history %}

{{
    config(
        unique_key = 'query_id',
        strategy   = 'timestamp',
        updated_at = 'end_time'
    )
}}

select * from {{ source('snowflake_meta', 'query_history') }}

{% endsnapshot %}