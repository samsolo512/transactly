with src_tc_user_agent_subscription_tier as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'user_agent_subscription_tier') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    u.id
    ,u.user_id
    ,u.agent_subscription_tier_id
    ,u.start_date
    ,u.price
    ,u.end_date
from
    src_tc_user_agent_subscription_tier u
where
    _fivetran_deleted = 'FALSE'
