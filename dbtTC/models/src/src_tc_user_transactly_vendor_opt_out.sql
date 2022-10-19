with src_tc_user_transactly_vendor_opt_out as(
    select *
    from {{ source('tc', 'user_transactly_vendor_opt_out') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    user_id
    ,created as created_date
    ,vendor_type_id
from src_tc_user_transactly_vendor_opt_out
where _fivetran_deleted = 'FALSE'
