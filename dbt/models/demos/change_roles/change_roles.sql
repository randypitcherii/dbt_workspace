{{ 
    config(
        pre_hook='use role ' ~ target.role , 
        post_hook='use role ' ~ target.role 
    ) 
}}

select 1 as my_column
