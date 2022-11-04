-- fact_revenue

create or replace table data_warehouse.fact_task (
    transaction_id int
    ,street string
    ,state string
    ,zip string
    ,assigned_to_name string
    ,due_date date
    ,aging_days int
    ,completed_date date
    ,text string
    ,completed_flag int
    ,private_flag int
    ,tc_agent_first_name string
    ,tc_agent_last_name string
    ,order_status string
    ,transaction_status string
    ,tc_staff_flag int
)