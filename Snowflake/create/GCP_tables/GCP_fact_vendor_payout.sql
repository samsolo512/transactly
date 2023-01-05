-- fact_vendor_payout

create or replace table data_warehouse.fact_vendor_payout (
    vendor_payout_name string
    ,stage string
    ,opportunity_id string
    ,opportunity_name string
    ,product_name string
    ,contact_full_name string
    ,opportunity_close_date date
    ,vendor_payout_date date
    ,vendor_payout_amount numeric(10,2)
)
