with 

deletes_raw as (
    select 
        *,
        last_value(dbt_valid_to) over (partition by id order by dbt_valid_to asc) is not null as is_deleted
    
    from {{ ref('hard_deletes__snapshot')}}
),

deletes as (
    select 
        *
    
    from deletes_raw

    where is_deleted
),

current_values as (
    select 
        *,
        false as is_deleted
    
    from {{ ref('hard_deletes__snapshot')}}

    where dbt_valid_to is null
),

current_values_with_soft_deletes as (
    select * from deletes
    union all
    select * from current_values
)

select * from current_values_with_soft_deletes