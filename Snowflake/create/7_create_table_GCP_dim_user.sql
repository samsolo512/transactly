-- dim_user

create or replace table business-analytics-337515.data_warehouse.dim_user (
    user_id int
    ,first_name string
    ,last_name string
    ,fullname string
    ,email string
    ,brokerage string
    ,subscription_level string
    ,transaction_coordinator_status string

    -- flags
    ,pays_at_title_flag int
    ,tc_client_flag int
    ,eligible_for_clients_flag int
    ,tc_agent_flag int
    ,diy_flag int

    -- dates
    ,start_date date
    ,days_between_start_date_and_first_order_date int
    ,last_order_placed date
    ,last_order_due date
    ,tier_3 date
    ,tier_2 date
    ,tier_1 date
    ,first_order_placed date
    ,first_order_closed date
    ,fifth_order_closed date
)
