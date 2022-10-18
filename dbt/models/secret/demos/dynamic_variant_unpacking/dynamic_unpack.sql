select 
    *, 
    {{unpack_json(ref('raw_json'), 'json')}} 
    
from {{ref('raw_json')}}