{{ 
    config(
        pre_hook='use role transformer', 
        post_hook='use role ' ~ target.role 
    ) 
}}

select 1 as my_column
