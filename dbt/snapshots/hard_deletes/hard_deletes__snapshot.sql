{% snapshot hard_deletes__snapshot %}

{{
    config(
      target_database=target.database,
      target_schema=target.schema,
      unique_key='order_id',
      check_cols=['color', 'status', 'ORDER_DATE'],
      strategy='check',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ ref('hard_deletes_source') }}

{% endsnapshot %}
