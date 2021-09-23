with tables_to_validate as (
    select *
    from {{target.database}}.information_schema.columns
    where 
        table_schema = UPPER('{{target.schema}}')
),

columns as (
    select 
        table_catalog || '.' || table_schema || '.' || table_name as identifier,
        array_agg(column_name) as column_names
    from tables_to_validate
    group by identifier
)

select * from columns where not ARRAY_CONTAINS('id', column_names)