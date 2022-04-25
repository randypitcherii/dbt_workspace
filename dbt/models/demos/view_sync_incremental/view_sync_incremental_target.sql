{{
    config(
        materialized='view_sync_incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}

with language_data as (
    select 1 as id, 'english' as content_display_name
    union all
    select 2 as id, 'spanish' as content_display_name
),

assets as (
    select 1 as id, 'foo' as content, 1 as language_id, 'active' as status
    union all select 2 as id, 'bar' as content, 1 as language_id, 'deactivated' as status
    --union all select 3 as id, 'baz' as content, 2 as language_id, 'active' as status
    union all select 4 as id, 'qux' as content, 2 as language_id, 'deactivated' as status
),

combined as (
select a.*,
       b.content_display_name as language 
from assets a 
left join language_data b
 on a.language_id = b.id
)

select * 
from combined
