select 
    {{ dbt_utils.surrogate_key(['order_id', 'dbt_scd_id']) }} as surrogate_key,
    *

    
from {{ ref('hard_deletes__snapshot')}}

where dbt_valid_to is null

order by order_id, dbt_updated_at