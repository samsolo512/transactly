-- src_tc_order

with src_tc_order as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'tc_order') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    o.id as order_id
    ,o.transaction_id
    ,o.agent_id
    ,case
        when o.created >= '1/18/2023' then cast(dateadd(hour, -6, o.created) as date)
        else cast(o.created as date)
        end as created_date
    ,o.assigned_tc_id
    ,o.status as order_status
    ,o.type as order_type
    ,o.address
    ,o.state
    ,o.side_id as order_side_id
    ,o.order_data
    ,o.assigned_tc_office_id
    ,o._fivetran_synced last_sync
    ,o.agent_office_id
    ,o.city
from
    src_tc_order o
where
    _fivetran_deleted = 'FALSE'
