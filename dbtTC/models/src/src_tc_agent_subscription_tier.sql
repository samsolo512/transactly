with src_tc_agent_subscription_tier as(
    select *
    from {{ source('tc', 'agent_subscription_tier') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    u.id
    ,u.name
from
    src_tc_agent_subscription_tier u
where
    _fivetran_deleted = 'FALSE'
