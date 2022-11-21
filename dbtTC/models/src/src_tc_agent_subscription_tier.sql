with src_tc_agent_subscription_tier as(
    select *
    from {{ source('transactly_app_production_transactly_app_production_rec_accounts', 'agent_subscription_tier') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    u.id
    ,u.name
from
    src_tc_agent_subscription_tier u
where
    _fivetran_deleted = 'FALSE'
