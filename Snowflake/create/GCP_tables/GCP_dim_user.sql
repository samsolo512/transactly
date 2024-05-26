-- dim_user

create or replace table data_warehouse.dim_user (
    user_pk int
    ,user_id int
    ,first_name string
    ,last_name string
    ,fullname string
    ,email string
    ,brokerage string
    ,office_id int
    ,office_name string
    ,subscription_level string
    ,transaction_coordinator_status string

    -- agent address
    ,address string
    ,address2 string
    ,original_sales_rep_name string

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

    ,anniversary_1_yr_1st_order_placed date
    ,days_since_last_order_placed int
    ,days_since_last_order_placed_over_90_flag int
    ,total_closed_orders int
    ,total_placed_orders int

    ,customer_id string
    ,updated_date date
    ,has_customer_id_flag int
    ,has_agent_acct_credentials_flag int
    ,is_user_vendor_flag int
    ,user_vendor_last_updated date
    ,auto_payment_flag int

    -- ledger
    ,ledger_id int
    ,ledger_created_date date
    ,ledger_credit_balance int
    ,ledger_updated_date date

    ,contact_owner string
)
