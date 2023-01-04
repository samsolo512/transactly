-- fact_opportunity

create or replace table data_warehouse.fact_opportunity (
    opportunity_name string
    ,opportunity_line_item_name string
    ,state string
    ,street string
    ,account_name string
    ,lead_email string
    ,owner_name string
    ,created_date date
    ,close_date date
    ,days_to_close int
    ,days_since_created int
    ,last_stage_change_date date
    ,agent_name string
    ,agent_email string
    ,product_name string
    ,product_family string
    ,revenue_connection_flag int
    ,unpaid_connection_flag int
    ,stage string
    ,contact_email string
    ,contact_id string
    ,revenue numeric(18,2)
)
