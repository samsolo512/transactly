-- fact_transaction

create or replace table data_warehouse.fact_transaction (
    user_id int
    ,transaction_id int
    ,fullname string
    ,brokerage string
    ,created_date date
    ,closed_date date
    ,diy_flag int
    ,diy_flag_all_transaction_statuses int
    ,first_order_placed date
    ,tc_staff_flag int
    ,HS_lead_status string
    ,agent_pays numeric
    ,office_pays numeric
    ,total_fees numeric
)