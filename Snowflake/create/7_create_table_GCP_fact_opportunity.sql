-- fact_opportunity

create or replace table data_warehouse.fact_opportunity (
    opportunity_name string
    ,state string
    ,street string
    ,account_name string
    ,opportunity_owner string
    ,email string
    ,close_date date
    ,product_name string
    ,product_family string
    ,revenue_connection_flag int
    ,unpaid_connection_flag int
    ,stage string
    ,is_won_flag int
    ,revenue numeric(18,2)
)

