with src_tc_address as(
    select *
    from {{ source('tc', 'address') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    a.id as address_id
    ,regexp_replace(a.address_line_1, '[\r\n]', ' ') as street
    ,regexp_replace(a.city, '[\r\n]', ' ') as city
    ,regexp_replace(a.state, '[\r\n]', ' ') as state
    ,a.zip
from src_tc_address a
where _fivetran_deleted = 'FALSE'
