-- dim_lead

create or replace table data_warehouse.dim_lead (
    first_name string
    ,last_name string
    ,name string
    ,company string
    ,street string
    ,city string
    ,state string
    ,postal_code string
    ,country string
    ,full_address string
    ,mobile_phone string
    ,email string
    ,lead_source string
    ,created_date date
    ,owner_name string
)