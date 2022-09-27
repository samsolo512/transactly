-- fact_opportunity

create or replace table data_warehouse.fact_opportunity (
    opportunity_name string
    ,revenue numeric(18,2)
    ,close_date date
    ,product_name string
    ,product_family string
    ,state string
    ,street string
    ,account_name string
    ,stage string
    ,is_won_flag int
    ,revenue_connection_flag int
    ,unpaid_connection_flag int
    ,opportunity_owner string
    ,contact_id string
    ,email string
)
