
{{ config(
    post_hook=[
      "{{unload_dim_user_to_GCP()}}"
    ]
) }}

with
    dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,final as(
        select
            user_id
            ,first_name
            ,last_name
            ,fullname
            ,email
            ,brokerage
            ,subscription_level
            ,transaction_coordinator_status as lead_status

            -- flag
            ,pays_at_title_flag
            ,eligible_for_clients_flag
            ,tc_staff_flag
            ,tc_client_flag
--             ,diy_flag
            ,self_procured_flag

            -- dates
            ,start_date
            ,days_between_start_date_and_first_order_date
            ,last_order_placed
            ,last_order_due
            ,tier_3
            ,tier_2
            ,tier_1
            ,first_order_placed
            ,first_order_closed
            ,fifth_order_closed
        from dim_user
    )

select * from final
