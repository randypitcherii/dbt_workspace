select 
    warehouse_name, 
    warehouse_id,
    {{pack_json(ref('stg_warehouse_metering_history'))}} as json
    
from {{ref('stg_warehouse_metering_history')}}