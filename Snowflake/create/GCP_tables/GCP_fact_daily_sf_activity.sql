-- fact_daily_sf_activity

create or replace table data_warehouse.fact_daily_sf_activity (
    date date
    ,leads_created int
    ,leads_converted int
    ,opportunities_created int
    ,opportunities_created_amount int
    ,opportunities_won int
    ,opportunities_won_amount int
    ,opportunities_lost int
    ,opportunities_lost_amount int
)
