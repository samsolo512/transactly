-- GCP_user_agent_difference

create or replace table data_warehouse.user_agent_difference (
    transaction_street string
    ,transaction_city string
    ,transaction_state string
    ,transaction_zip string
    ,user_first_name string
    ,user_last_name string
    ,user_email string
    ,office_name string
    ,assigned_tc_first_name string
    ,assigned_tc_last_name string
    ,assigned_tc_email string
    ,created_by_first_name string
    ,created_by_last_name string
    ,created_by_email string
)