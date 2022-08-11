-- fact_opportunity

create or replace table business-analytics-337515.data_warehouse.fact_opportunity (
    opportunity_name string
    ,total_revenue numeric
    ,close_date date
    ,product_name string
    ,product_family string
    ,state string
    ,street string
    ,account_name string
    ,stage string
    ,is_won_flag int
)
