-- fact_transaction_member

create or replace table data_warehouse.fact_transaction_member (
    street string,
    city string,
    state string,
    zip string,
    member_first_name string,
    member_last_name string,
    member_email string,
    member_role string,
    member_office_name string,
    assigned_tc_first_name string,
    assigned_tc_last_name string,
    assigned_tc_email string,
    transaction_side string,
    member_side string,
    tc_buyer_as_connect_lead_flag int,
    diy_flag int,
    connect_lead_created_date date
)
