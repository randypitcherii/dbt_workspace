select 
    day, 
    zodiac_sign,
    {{pack_json(ref('stg_zodiac'))}} as json
    
from {{ref('stg_zodiac')}}

-- {{this}}