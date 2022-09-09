
{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_order')}}"
    ]
) }}

with
    fact_order as(
        select *
        from {{ ref('fact_order') }}
    )

    ,dim_transaction as(
        select *
        from {{ ref('dim_transaction') }}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,dim_order as(
        select *
        from {{ ref('dim_order') }}
    )

    ,dim_date as(
        select *
        from {{ ref('dim_date') }}
    )

    ,final as(
        select
            o.order_id
            ,o.transaction_id
            ,t.street
            ,t.city
            ,t.state
            ,t.zip
            ,u.user_id
            ,u.first_name
            ,u.last_name
            ,ua.first_name as assigned_tc_first_name
            ,ua.last_name as assigned_tc_last_name
            ,o.order_type
            ,o.order_side
            ,o.order_status
            ,o.status_changed_date
            ,cldate.date_id as closed_date
            ,fact.closed_sequence
            ,crdate.date_id as created_date
            ,fact.placed_sequence

        from
            fact_order fact
            join dim_order o on fact.order_pk = o.order_pk
            join dim_transaction t on fact.transaction_pk = t.transaction_pk
            join dim_user ua on fact.assigned_tc_pk = ua.user_pk
            join dim_user uc on fact.created_by_pk = uc.user_pk
            join dim_user u on fact.user_pk = u.user_pk
            join dim_date crdate on fact.created_date_pk = crdate.date_pk
            join dim_date cldate on fact.closed_date_pk = cldate.date_pk
            join dim_date closed_date on fact.closed_date_pk = closed_date.date_pk
    )

select * from final