{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_task')}}"
    ]
) }}


with
    fact_task as(
        select *
        from {{ ref('fact_task') }}
    )

    ,dim_task as(
        select *
        from {{ ref('dim_task') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,final as(
        select
            t.transaction_id
            ,t.street
            ,t.state
            ,task.assigned_to_name
            ,task.due_date
            ,task.text
            ,task.completed_flag
            ,task.private_flag
            ,t.tc_agent_first_name
            ,t.tc_agent_last_name
            ,t.order_status
            ,t.transaction_status
            ,task.tc_staff_flag

        from
            fact_task fact
            join dim_task task on fact.task_pk = task.task_pk
            join dim_transaction t on fact.transaction_pk = t.transaction_pk
        where
            task.due_date >= dateadd(day, -90, getdate())  -- limit output for Google Sheets
    )

select * from final