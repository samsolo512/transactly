
{{ config(
    post_hook=[
      "{{unload_to_GCP('GCP_dim_user')}}"
    ]
) }}

with
    dim_user as(
        select *
        from {{ ref('dim_user') }}
    )

    ,final as(
        select
            user_pk
            ,user_id
            ,first_name
            ,last_name
            ,fullname
            ,email
            ,brokerage
            ,office_id
            ,office_name
            ,subscription_level
            ,transaction_coordinator_status
            ,address
            ,address2
            ,original_sales_rep_name
        
            -- flags
            ,pays_at_title_flag
            ,eligible_for_clients_flag
            ,tc_staff_flag
            ,tc_client_flag
            ,lead_flag
            ,self_procured_flag
        
            -- HubSpot fields
            ,HS_agent_type
            ,transactly_home_insurance_vendor_status
            ,transactly_utility_connection_vendor_status
        
            ,user_created_date
            ,start_date
            ,days_between_start_date_and_first_order_date
            ,tier_3
            ,tier_2
            ,tier_1

            -- orders
            ,last_order_due
            ,first_order_placed
            ,last_order_placed
            ,first_order_closed
            ,second_order_closed
            ,third_order_closed
            ,fourth_order_closed
            ,fifth_order_closed

            ,anniversary_1_yr_1st_order_placed
            ,days_since_last_order_placed
            ,days_since_last_order_placed_over_90_flag
            ,total_closed_orders
            ,total_placed_orders

        from dim_user
    )

select * from final
