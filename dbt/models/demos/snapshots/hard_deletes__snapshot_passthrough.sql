/* select 
    *
    
from {{ ref('hard_deletes__snapshot')}}
 */


 select 
        id,
        first_value(dbt_valid_to) over (partition by id order by dbt_valid_to asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as cnt
    
    from {{ ref('hard_deletes__snapshot')}}