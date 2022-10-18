{{
    config(
        materialized='view',
        pre_hook='{{clean_workspace(dry_run=True)}}'
    )
}}

select 'all clean!' as msg 