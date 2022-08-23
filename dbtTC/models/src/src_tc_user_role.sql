with src_tc_user_role as(
    select *
    from {{ source('fivetran', 'user_role') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    ur.user_id
    ,ur.role_id
from src_tc_user_role ur

