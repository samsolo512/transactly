with src_tc_transaction_status as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'transaction_status') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    t.id as transaction_status_id
    ,t.name as status
from src_tc_transaction_status t
where _fivetran_deleted = 'FALSE'
