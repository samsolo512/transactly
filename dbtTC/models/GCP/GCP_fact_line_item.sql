
{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_fact_line_item')}}"
    ]
) }}

with
    fact_line_item as(
        select *
        from {{ ref('fact_line_item') }}
    )

    ,dim_line_item as(
        select *
        from {{ ref('dim_line_item') }}
    )

    ,dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,dim_order as(
        select *
        from {{ ref('dim_order') }}
    )

    ,dim_office as(
        select *
        from {{ ref('dim_office') }}
    )

    ,dim_date as(
        select *
        from {{ ref('dim_date') }}
    )

    ,final as(
        select
            o.address
            ,o.state
            ,line.description
            ,line.status
            ,line.agent_pays as agent_pays_amt
            ,line.office_pays as office_pays_amt
            ,assigned.user_id as assigned_tc_id
            ,assigned.fullname as assigned_tc_name
            ,o.order_type
            ,o.order_side
            ,o.order_id
            ,o.transaction_id
            ,line.paid
            ,user.pays_at_title_flag
            ,line.tc_paid
            ,user.user_id as client_id
            ,user.fullname
            ,user.brokerage as client_brokerage
            ,user.tier_1 as tier_1_date  -- due date of 5th sale
            ,user.tier_2 as tier_2_date  -- due date of 1st sale
            ,user.tier_3 as tier_3_date  -- user created date
            ,line.created_date
            ,fact.due_date
            ,o.closing_date
            ,line.cancelled_date as cancelled_date
            ,user.last_order_placed as last_order_placed_date
            ,user.last_order_due as last_order_due_date
            ,user.first_order_placed as first_order_placed_date
            ,user.first_order_closed as first_order_closed_date
            ,user.fifth_order_closed as fifth_order_closed_date
            ,ofc.office_name
            ,o.order_status
            ,to_timestamp(greatest(line.last_sync, o.last_sync)) as last_sync
            ,user.subscription_level
            ,assigned.transaction_coordinator_status
            ,assigned.eligible_for_clients_flag
            ,user.start_date
            ,user.days_between_start_date_and_first_order_date
            ,line.total_fees
            ,o.city
            ,o.status_changed_date
            ,user.second_order_closed as second_order_closed_date
            ,user.third_order_closed as third_order_closed_date
            ,user.fourth_order_closed as fourth_order_closed_date
            ,user.contact_owner
            ,fact.placed_sequence
            ,fact.closed_sequence
            ,user.original_sales_rep_name
            ,line.line_item_id

        from
            fact_line_item fact
            join dim_line_item line on fact.line_item_pk = line.line_item_pk
            left join dim_user user on fact.user_pk = user.user_pk
            left join dim_order o on fact.order_pk = o.order_pk
            left join dim_user assigned on fact.assigned_tc_pk = assigned.user_pk
            left join dim_office ofc on fact.office_pk = ofc.office_pk
            left join dim_date line_item_created_date on fact.created_date_pk = line_item_created_date.date_pk
            left join dim_date line_item_due_date on fact.created_date_pk = line_item_due_date.date_pk
            left join dim_date line_item_cancelled_date on fact.created_date_pk = line_item_cancelled_date.date_pk
            left join dim_date closed_date on cast(fact.closed_date_pk as date) = closed_date.date_id
    )

select * from final