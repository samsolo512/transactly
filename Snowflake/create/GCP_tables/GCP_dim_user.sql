-- dim_user

create or replace table data_warehouse.dim_user (
    user_pk int
    ,user_id int
--     ,lead_id string
    ,first_name string
    ,last_name string
    ,fullname string
    ,email string
    ,brokerage string
    ,office_id int
    ,subscription_level string
    ,transaction_coordinator_status string
--     ,contact_owner string
--     ,contact_id string

    -- agent address
--     ,agent_name string
--     ,agent_email string
    ,address string
    ,address2 string
    ,original_sales_rep_name string

    -- lead address
--     ,lead_street string
--     ,lead_city string
--     ,lead_state string
--     ,lead_zip string
--     ,lead_country string
--     ,full_address string

    --flags
    ,pays_at_title_flag int
    ,eligible_for_clients_flag int
    ,tc_staff_flag int
    ,tc_client_flag int
    ,lead_flag int
    ,self_procured_flag int

    --HubSpot fields
    ,HS_agent_type string
    ,transactly_home_insurance_vendor_status string
    ,transactly_utility_connection_vendor_status string

    -- dates
    ,user_created_date date
--     ,lead_created_date date
    ,start_date date
    ,days_between_start_date_and_first_order_date int
    ,tier_3 datetime
    ,tier_2 datetime
    ,tier_1 datetime
    ,last_order_due datetime
    ,first_order_placed date
    ,last_order_placed date
    ,first_order_closed date
    ,second_order_closed date
    ,third_order_closed date
    ,fourth_order_closed date
    ,fifth_order_closed date
)
