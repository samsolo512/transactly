-- src_tc_agent_acct_credentials

with src_tc_agent_acct_credentials as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'agent_acct_credentials') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    a.id as user_id
from 
    src_tc_agent_acct_credentials a
where 
    _fivetran_deleted = 'FALSE'
