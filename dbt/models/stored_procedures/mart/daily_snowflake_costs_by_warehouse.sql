select 
    sum(credits_used) * 3.00 as cost, 
    end_time::date           as end_date,
    warehouse_name

from {{ ref('fct_warehouse_metering_history') }}

group by end_date, warehouse_name

order by end_date desc