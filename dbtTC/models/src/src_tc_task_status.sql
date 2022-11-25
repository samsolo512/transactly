with src_tc_task_status as(
    select *
    from {{ source('gcp_prod_gcp_prod_prod', 'task_status') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    t.id as status_id
    ,t.name as status_name
from
    src_tc_task_status t
where
    _fivetran_deleted = 'FALSE'
