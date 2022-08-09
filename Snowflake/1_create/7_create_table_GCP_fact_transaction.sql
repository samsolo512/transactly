--fact_transaction

create or replace table business-analytics-337515.data_warehouse.fact_transaction (
    user_id int
    ,transaction_id int
    ,fullname string
    ,brokerage string
    ,created_date date
    ,closed_date date
    ,diy_flag int
)