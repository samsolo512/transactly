with src_tc_task as(
    select *
    from {{ source('transactly_app_production_transactly_app_production_rec_accounts', 'task') }}
    where lower(_fivetran_deleted) = 'false'
)

select
    t.id as task_id
    ,to_date(t.due_date) as due_date
    ,{{ field_clean('t.text') }} as text
    ,t.status_id
    ,to_date(t.completed_date) as completed_date
    ,case
        when t.completed = 'TRUE' then 1
        when t.completed = 'FALSE' then 0
        else null
        end as completed_flag
    ,t.category
    ,t.transaction_id
    ,case
        when t.private = 'TRUE' then 1
        when t.private = 'FALSE' then 0
        end as private_flag
    ,t.assigned_to_id
from
    src_tc_task t
where
    _fivetran_deleted = 'FALSE'
