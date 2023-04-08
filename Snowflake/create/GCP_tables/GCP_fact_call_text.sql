-- fact_call_text

create or replace table data_warehouse.fact_call_text (
    contact_method string
    ,created_date datetime
    ,caller_id string
    ,phone string
    ,direction string
    ,call_duration_in_seconds int
    ,call_duration string
    ,user string
    ,activity_name string
    ,source string
    ,message string
    ,message_id string
    ,response_time string
    ,contact_name string
)
