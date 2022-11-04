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

    ,final as(
        select
            working.seq_dim_task.nextval as task_pk
            ,t.task_id
            ,t.due_date
            ,datediff(day, due_date, getdate()) as aging_days
            ,t.text
            ,s.status_name
            ,t.completed_date
            ,t.completed_flag
            ,t.category
            ,t.private_flag
            ,u.fullname as assigned_to_name
            ,u.tc_staff_flag

        from
            src_tc_task t
            left join src_tc_task_status s on t.status_id = s.status_id
            left join dim_user u on t.assigned_to_id = u.user_id

    )

select * from final

-- select task_id, count(1) from final group by task_id order by count(1) desc

