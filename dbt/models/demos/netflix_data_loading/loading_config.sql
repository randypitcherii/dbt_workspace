
select current_timestamp as updated_at, 'randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage' as stage_name, 'string' as column_type, 'show_id'      as column_name, 1  as column_position
union all
select current_timestamp as updated_at, 'randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage' as stage_name, 'string' as column_type, 'type'         as column_name, 2  as column_position
union all
select current_timestamp as updated_at, 'randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage' as stage_name, 'string' as column_type, 'title'        as column_name, 3  as column_position
union all
select current_timestamp as updated_at, 'randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage' as stage_name, 'string' as column_type, 'director'     as column_name, 4  as column_position
union all
select current_timestamp as updated_at, 'randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage' as stage_name, 'string' as column_type, 'cast'         as column_name, 5  as column_position
union all
select current_timestamp as updated_at, 'randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage' as stage_name, 'string' as column_type, 'country'      as column_name, 6  as column_position
union all
select current_timestamp as updated_at, 'randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage' as stage_name, 'string' as column_type, 'date_added'   as column_name, 7  as column_position
union all
select current_timestamp as updated_at, 'randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage' as stage_name, 'string' as column_type, 'release_year' as column_name, 8  as column_position
union all
select current_timestamp as updated_at, 'randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage' as stage_name, 'string' as column_type, 'rating'       as column_name, 9  as column_position
union all
select current_timestamp as updated_at, 'randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage' as stage_name, 'string' as column_type, 'duration'     as column_name, 10 as column_position
union all
select current_timestamp as updated_at, 'randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage' as stage_name, 'string' as column_type, 'listed_in'    as column_name, 11 as column_position
union all
select current_timestamp as updated_at, 'randy_pitcher_workspace_raw.netflix_loading.netflix_blob_stage' as stage_name, 'string' as column_type, 'description'  as column_name, 12 as column_position