-- fact_order

create or replace table data_warehouse.fact_order (
    order_id int
    ,transaction_id int
    ,office_name string
    ,order_type string
    ,order_side string
    ,order_status string
    ,status_changed_date date
    ,street string
    ,city string
    ,state string
    ,zip string
    ,user_id int
    ,user_first_name string
    ,user_last_name string
    ,user_email string
    ,assigned_tc_user_id int
    ,assigned_tc_first_name string
    ,assigned_tc_last_name string
    ,assigned_tc_email string
    ,created_by_user_id int
    ,created_by_first_name string
    ,created_by_last_name string
    ,created_by_email string
    ,closed_date date
    ,created_date date
)

