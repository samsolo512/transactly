
{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_transaction')}}"
    ]
) }}

with
    fact_transaction as(
        select *
        from {{ ref('fact_transaction') }}
    )

    ,dim_order as(
        select *
        from {{ ref('dim_order') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,final as(
        select
            usr.user_id
            ,tran.transaction_id
            ,usr.fullname
            ,usr.brokerage
            ,tran.created_date
            ,tran.closed_date
            ,tran.contract_closing_date
            ,tran.diy_flag
            ,tran.transaction_side
            ,tran.street
            ,tran.transaction_status

            ,tran.diy_flag_all_transaction_statuses
            ,usr.first_order_placed
            ,usr.tc_staff_flag
            ,usr.HS_lead_status

            ,fact.agent_pays
            ,fact.office_pays
            ,fact.total_fees

        from
            fact_transaction fact
            left join dim_transaction tran on fact.transaction_pk = tran.transaction_pk
            join dim_user usr on fact.user_pk = usr.user_pk
            join dim_order o on fact.order_pk = o.order_pk
    )

select * from final
