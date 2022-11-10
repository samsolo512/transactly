with src_sf_account as(
    select *
    from {{ source('sf', 'account') }}
)

select
    a.partner_recruiter_rate_c
    ,a.id as account_id
    ,a.name as account_name
    ,a.owner_id
    ,a.partner_status_c as partner_status
from src_sf_account a
where
    is_deleted = 'FALSE'