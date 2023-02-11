-- fact_text

create or replace table data_warehouse.fact_text (
    user_name string
    ,message_id string
    ,text_body string
    ,to_number string
    ,from_number string
    ,created_date string
    ,direction string
    ,response_time string
)
