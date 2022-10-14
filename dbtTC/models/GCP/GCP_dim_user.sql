
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
            ,lead_id
            ,first_name
            ,last_name
            ,fullname
            ,email
            ,brokerage
            ,subscription_level
            ,transaction_coordinator_status
            ,contact_owner
            ,contact_id
        
            -- agent address
            ,agent_name
            ,agent_email
            ,address
            ,address2
            ,original_sales_rep_name
        
            -- lead address
            ,lead_street
            ,lead_city
            ,lead_state
            ,lead_zip
            ,lead_country
            ,full_address
        
            --flags
            ,pays_at_title_flag
            ,eligible_for_clients_flag
            ,tc_staff_flag
            ,tc_client_flag
            ,lead_flag
            ,self_procured_flag
        
            --HubSpot fields
            ,HS_agent_type
            ,transactly_home_insurance_vendor_status
            ,transactly_utility_connection_vendor_status
        
            --s
            ,user_created_date
            ,lead_created_date
            ,start_date
            ,days_between_start_date_and_first_order_date
            ,tier_3
            ,tier_2
            ,tier_1
            ,last_order_due
            ,first_order_placed
            ,last_order_placed
            ,first_order_closed
            ,second_order_closed
            ,third_order_closed
            ,fourth_order_closed
            ,fifth_order_closed
        from dim_user
    )

select * from final
