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
    ,mobile_phone string
    ,email string
    ,lead_source string
    ,created_date date
    ,owner_first_name string
    ,owner_last_name string
    ,owner_name string
    ,owner_title string
    ,owner_street string
    ,owner_city string
    ,owner_postal_code string
    ,owner_country string
    ,owner_email string
    ,owner_phone string
    ,owner_mobile_phone string
    ,owner_is_active_flag int
)