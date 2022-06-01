{{ 
    config(
        pre_hook='use role ' ~ target.role , 
        post_hook='use role public' 
    ) 
}}

select 2 as my_column
