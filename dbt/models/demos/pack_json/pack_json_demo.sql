select 
    warehouse_name, 
    warehouse_id,
    {{pack_json()}} as new_name_for_variant
    
from {{ref('stg_warehouse_metering_history')}}