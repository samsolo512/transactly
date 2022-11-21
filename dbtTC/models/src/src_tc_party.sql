with src_tc_party as(
    select *
    from {{ source('transactly_app_production_transactly_app_production_rec_accounts', 'party') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    p.id as party_id
    ,p.name as party_name
from
    src_tc_party p
where
    _fivetran_deleted = 'FALSE'
