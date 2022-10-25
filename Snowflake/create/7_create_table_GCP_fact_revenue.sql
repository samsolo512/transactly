-- fact_revenue

create or replace table data_warehouse.fact_revenue (
    opportunity_id string
    ,vendor_payout_id string
    ,user_id string
    ,agent_name string
    ,lead_agent_flag int
    ,tc_agent_flag int
    ,tc_created_date date
    ,lead_created_date date
    ,account_name string
    ,revenue_type string
    ,date datetime
    ,opportunity_revenue numeric(10,2)
    ,transactly_revenue numeric(10,2)
    ,vendor_payout_amount numeric(10,2)
    ,total_revenue numeric(10,2)
)
