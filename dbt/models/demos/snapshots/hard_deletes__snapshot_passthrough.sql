{% set distant_past='TIMESTAMP("1960-01-01 00:00:00+00")'%}
{% set distant_future='TIMESTAMP("2100-01-01 00:00:00+00")'%}
{% set min_valid_from='(select min(dbt_valid_from) from ' ~ ref('hard_deletes__snapshot') ~ ')'%}

 select 
    *,

    if(
        dbt_valid_from = {{ min_valid_from }},
        {{distant_past}},
        dbt_valid_from
    ) as valid_from_with_distant_past,

    if(
        dbt_valid_to is null,
        {{distant_future}},
        dbt_valid_to
    ) as valid_to_with_distant_future


    
from {{ ref('hard_deletes__snapshot')}}