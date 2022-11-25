with src_tc_offer_group as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'offer_group') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    id as offer_group_id
    ,transaction_id
    ,status_id
    ,split_part(agent_full_name, ' ', 1) as first_name
    ,split_part(agent_full_name, ' ', 2) as last_name

from src_tc_offer_group

where
    _fivetran_deleted = 'FALSE'

