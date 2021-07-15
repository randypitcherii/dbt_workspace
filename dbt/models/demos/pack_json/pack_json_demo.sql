select 
    warehouse_name, 
    warehouse_id,
    {{pack_json(ref('stg_warehouse_metering_history'), except=['json'])}} as json
    
from {{ref('stg_warehouse_metering_history')}}

-- {{this}}