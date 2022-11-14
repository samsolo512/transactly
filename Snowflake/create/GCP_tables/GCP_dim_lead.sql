-- dim_lead

create or replace table data_warehouse.dim_lead (
    lead_id string
    ,first_name string
    ,last_name string
    ,name string
    ,company string
    ,street string
    ,city string
    ,state string
    ,zip string
    ,country string
    ,full_address string
    ,phone string
    ,email string
    ,lead_source string
    ,lead_created_date date
    ,agent_name string
    ,agent_email string
    ,electricity string
    ,sewer string
    ,trash string
    ,water string
    ,gas string
    ,internet string
    ,account_name string
    ,account_owner string
)
