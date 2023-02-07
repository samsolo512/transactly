-- fact_opportunity

create or replace table data_warehouse.fact_opportunity (
    opportunity_name string
    ,opportunity_line_item_name string
    ,opportunity_id string
    ,stage string
    ,lease_start_date string
    ,opportunity_owner_name string

    -- account and product
    ,account_name string
    ,product_name string
    ,product_family string
    ,vendor string

    -- lead
    ,lead_street string
    ,lead_state string
    ,lead_phone string
    ,lead_mobile_phone string
    ,lead_email string
    ,lead_owner_name string
    ,lead_agent_name string
    ,lead_agent_email string
    ,lead_week_date date
    
    -- contact
    ,contact_id string
    ,contact_full_name string
    ,contact_phone string
    ,contact_mobile_phone string
    ,contact_email string
    ,contact_owner_name string

    -- facts
    ,service_start_date date
    ,created_date date
    ,close_date date
    ,days_to_close int
    ,days_since_created int
    ,last_stage_change_date date
    ,revenue_connection_flag int
    ,unpaid_connection_flag int
    ,revenue numeric(18,2)
)
