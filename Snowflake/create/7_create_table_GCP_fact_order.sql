-- fact_order

create or replace table data_warehouse.fact_order (
    order_id int
    ,transaction_id int
    ,street string
    ,city string
    ,state string
    ,zip string
    ,user_id int
    ,first_name string
    ,last_name string
    ,assigned_tc_first_name string
    ,assigned_tc_last_name string
    ,order_type string
    ,order_side string
    ,order_status string
    ,status_changed_date date
    ,closed_date date
    ,closed_sequence int
    ,created_date date
    ,placed_sequence int
)