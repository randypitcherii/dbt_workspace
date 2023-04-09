{% macro bigquery__create_augmented_snapshot(snapshot_table, table_key) %}

    {% set distant_past       ="TIMESTAMP('1900-01-01 00:00:00+00')"%}
    {% set distant_future     ="TIMESTAMP('9999-12-31 23:59:59+00')"%}
    {% set dbt_valid_from_min =
        'min(dbt_valid_from) over (
            partition by ' ~ table_key ~ ' order by dbt_valid_from rows between unbounded preceding and unbounded following
        )'
    %}


    with

    snapshot_with_distant_past_and_distant_future_raw as (
        select
            *,

            if(
                dbt_valid_from = {{ dbt_valid_from_min }},
                {{ distant_past }},
                dbt_valid_from
            ) as valid_from_with_distant_past,

            if(
                dbt_valid_to is null,
                {{ distant_future }},
                dbt_valid_to
            ) as valid_to_with_distant_future

        from {{ snapshot_table }}
    ),


    snapshot_with_distant_past_and_distant_future as (
        select 
            {{ dbt_utils.star(snapshot_table) }},
            valid_from_with_distant_past,
            valid_to_with_distant_future
        
        from snapshot_with_distant_past_and_distant_future_raw
    ),


    snapshot_with_is_deleted as (
        select 
            *,
            first_value(dbt_valid_to) over (
                partition by {{table_key}} order by dbt_valid_to asc rows between unbounded preceding and unbounded following
            ) is not null as is_deleted
        
        from snapshot_with_distant_past_and_distant_future
    ),

    snapshot_with_current_values as (
        select 
            *,

            if(
                is_deleted or (dbt_valid_to is null),
                true,
                false
            ) as is_current
        
        from snapshot_with_is_deleted
    )

    select * from snapshot_with_current_values

{% endmacro %}