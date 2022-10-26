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

--     ,owner_name string
--     ,lead_partner_name string
--     ,contact_partner_name string
--     ,opportunity_partner_name string
--     ,contact_created_date date
--     ,opportunity_created_date date
--     ,opportunity_close_date date
--     ,opportunity_name string
--     ,stage string

)
