with src_tc_ledger as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'ledger') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    l.id as ledger_id
    ,to_date(l.created) as ledger_created_date
    -- ,l.deleted
    ,l.user_id
    ,l.credit_balance as ledger_credit_balance
    ,to_date(l.updated) as ledger_updated_date
    -- ,created_by_id
from
    src_tc_ledger l

