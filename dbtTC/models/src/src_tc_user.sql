with src_tc_user as(
    select *
    from {{ source('tc', 'user') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    u.id as user_id
    ,u.join_date
    ,u.is_active
    ,u.is_tc_client
    ,u.assigned_transactly_tc_id
    ,cast(u.last_online_date as date) as last_online_date
    ,trim({{ field_clean('u.first_name') }}) as first_name
    ,trim({{ field_clean('u.last_name') }}) as last_name
    ,concat({{ field_clean('u.first_name') }}, ' ', {{ field_clean('u.last_name') }}) as fullname
    ,u.email
    ,u.first_login
    ,cast(u.autopay_date as date) as autopay_date
    ,cast(u.created as date) as created_date
    ,u.google_user_id
    ,u.pays_at_title
    ,u.brokerage
    ,u.self_procured
    ,u.phone
    ,u.stripe_account_id
from
    src_tc_user u
where
    _fivetran_deleted = 'FALSE'
