with
    src_tc_task as(
        select *
        from {{ ref('src_tc_task') }}
    )

    ,dim_task as(
        select *
        from {{ ref('dim_task') }}
    )

    ,src_tc_transaction as(
        select *
        from {{ ref('src_tc_transaction') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,src_tc_task_status as(
        select *
        from {{ ref('src_tc_task_status') }}
    )

    ,final as(
        select
            task.task_pk
            ,trans.transaction_pk
        from
            src_tc_task t
            join dim_task task on t.task_id = task.task_id
            join src_tc_transaction tr on tr.transaction_id = t.transaction_id
            join dim_transaction trans on tr.transaction_id = trans.transaction_id
            left join src_tc_task_status s on t.status_id = s.status_id
    )

select * from final