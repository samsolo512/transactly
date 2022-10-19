-- fact_revenue

create or replace table data_warehouse.fact_revenue (
    lead_id string
    ,user_id int
    ,fullname string
    ,lead_flag int
    ,tc_client_flag int
    ,date datetime
    ,client_type string
    ,opportunity_revenue numeric(10,2)
    ,transactly_revenue numeric(10,2)
    ,vendor_payout_amount numeric(10,2)
    ,total_revenue numeric(10,2)
)