-- fact_call_chat

create or replace table data_warehouse.fact_call_chat (
    contact_method string
    ,created_date date
    ,caller_id string
    ,phone string
    ,direction string
    ,call_duration_in_seconds int
    ,call_duration string
    ,lead_name string
    ,lead_owner string
    ,call_twilio_client string
    ,activity_name string
)
