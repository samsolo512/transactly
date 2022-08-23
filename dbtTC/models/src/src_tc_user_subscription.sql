with src_tc_user_subscription as(
    select *
    from {{ source('fivetran', 'user_subscription') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    u.user_id
    ,u.level as subscription_level
from src_tc_user_subscription u

