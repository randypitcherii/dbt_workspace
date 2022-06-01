{{ 
    config(
        pre_hook='use role ' ~ target.role , 
        post_hook='use role public' 
    ) 
}}

select 1 as my_column
