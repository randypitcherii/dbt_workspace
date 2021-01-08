{% snapshot warehouse_metering_history__snapshot %}

{{
    config(
      target_database=target.database,
      target_schema=target.schema,
      unique_key="START_TIME_CENTRAL_TIME||'-'||WAREHOUSE_ID",

      strategy='timestamp',
      updated_at='watermark',
    )
}}

select * from {{ ref('stg_warehouse_metering_history') }}

{% endsnapshot %}