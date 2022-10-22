-- fact_revenue

create or replace table data_warehouse.fact_revenue (
    opportunity_id string
    ,vendor_payout_id string
    ,lead_id string
    ,user_id string
    ,fullname string
    ,lead_flag int
    ,tc_client_flag int
    ,client_type string
    ,agent_name string
    ,account_name string
    ,revenue_type string
    ,date datetime
    ,opportunity_revenue numeric(10,2)
    ,transactly_revenue numeric(10,2)
    ,vendor_payout_amount numeric(10,2)
    ,total_revenue numeric(10,2)
)
