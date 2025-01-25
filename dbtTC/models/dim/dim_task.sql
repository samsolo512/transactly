with
    src_tc_task as(
        select *
        from {{ ref('src_tc_task') }}
    )

    ,src_tc_transaction as(
        select *
        from {{ ref('src_tc_transaction') }}
    )

    ,src_tc_task_status as(
        select *
        from {{ ref('src_tc_task_status') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,final as(
        select distinct
            -- grain
            t.task_id

            ,t.due_date
            ,datediff(day, due_date, getdate()) as aging_days
            ,t.text
            ,s.status_name
            ,nvl(t.completed_date, tran.closed_date) as completed_date
            ,case
                when t.completed_flag = 1 then 1
                when t.completed_flag = 0 and tran.closed_date is null then 0
                when t.completed_flag = 0 and tran.closed_date is not null then 1
                else null
                end as completed_flag
            ,t.category
            ,t.private_flag
            ,u.fullname as assigned_to_name
            ,u.tc_staff_flag

        from
            src_tc_task t
            left join src_tc_task_status s on t.status_id = s.status_id
            left join dim_user u on t.assigned_to_id = u.user_id
            left join dim_transaction tran on t.transaction_id = tran.transaction_id

    )

select
    working.seq_dim_task.nextval as task_pk
    ,* 
from 
    final

