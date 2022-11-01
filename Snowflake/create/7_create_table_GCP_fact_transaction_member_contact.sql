 -- fact_transaction_member

create or replace table data_warehouse.fact_transaction_member (
    transaction_id int
    ,street string
    ,city string
    ,state string
    ,zip string
    ,agent_first_name string
    ,agent_last_name string
    ,agent_phone string
    ,tc_agent_first_name string
    ,tc_agent_last_name string
    ,tc_agent_phone string
    ,member_contact_first_name string
    ,member_contact_last_name string
    ,member_contact_role string
    ,member_contact_phone string
    ,member_contact_email string
    ,transaction_side string
    ,status string
    ,diy_flag int
    ,contract_closing_date date
    ,utility_transfer_status string
    ,utility_lead_sent_to string
    ,utility_notified_date date
    ,home_insurance_status string
    ,home_insurance_lead_sent_to string
    ,home_insurance_notified_date date
)
