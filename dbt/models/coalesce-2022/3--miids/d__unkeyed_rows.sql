

select
    b.*

from
    {{ ref('b__miid_unprocessed_rows') }} b left outer join {{ ref('c__keymap') }} c
    on b.natural_key = c.natural_key

where
    miid is null

{{
    config(
        materialized='table',
        post_hook="
        insert into {{ ref('a__miid') }}
        (natural_key, zodiac_sign, color, processed_at)
        (
            select natural_key, zodiac_sign, color, processed_at
            from {{this}}
        )
        "
    )
}}



