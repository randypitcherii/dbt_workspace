{{ 
    config(
        pre_hook='use role ' ~ target.role , 
        post_hook='use role ' ~ target.role 
    ) 
}}

select 2 as my_column
