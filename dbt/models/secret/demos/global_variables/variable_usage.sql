{% set local_var="'my_quoted_var'" %}

select 
    '{{var('my_cool_var')}}'::date as col_a,
    {{local_var}} as col_b