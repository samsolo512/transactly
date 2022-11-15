{{
    config(
        materialized = 'table'
    )
}}

with
    src_hs_object_properties as(
        select *
        from {{ source('hs', 'object_properties') }}
    )

select
    hs_object_id as contact_id
    ,firstname as first_name
    ,lastname as last_name
    ,brokerage_name
    ,to_number(case when transactly_user_id__app_ = '' then null else transactly_user_id__app_ end) as transactly_id
    ,cast(createdate as date) as created_date
    ,hs_lead_status as lead_status
    ,case when email = '' then null else email end as email
    ,original_sales_repr as original_sales_rep
    ,hubspot_owner_id as contact_owner
    ,hs_lifecyclestage_customer_date as became_a_customer
    ,renewal_date
    ,case when mobilephone = '' then null else mobilephone end as mobile_phone_number
    ,case when phone = '' then null else phone end as phone_number
    ,address
    ,address2
    ,city
    ,state_province
    ,zip
    ,company as company_name
    ,brokerage_growth_manager
    ,type
    ,eligible_for_clients
    ,transactly_home_insurance_vendor_status
    ,transactly_utility_connection_vendor_status

--     ,cast(hs_start_date as date) as hs_start_date
--     ,cast(start_date as date) as start_date
--     ,cast(tc_start_date as date) as tc_start_date

from(

    select
        objectid
        ,name
        ,value
    from src_hs_object_properties
    where
        objecttypeid = '0-1'
        and name in(
            'hs_object_id'  -- contact id
            ,'firstname'  -- first name
            ,'lastname'  -- last name
            ,'brokerage_name'  -- brokerage name
            ,'transactly_user_id__app_'  -- transactly user
            ,'createdate'  -- create date
            ,'hs_lead_status'  -- lead status
            ,'email'
            ,'original_sales_repr'  -- original sales rep
            ,'hubspot_owner_id'  -- contact owner
            ,'hs_lifecyclestage_customer_date'  -- became a customer
            -- is active (APP)
            -- assigned TC (APP)
            -- number of Active Transactions (APP)
            -- user number of orders (APP)
            ,'renewal_date'  -- renewal date (APP)
            -- agent role (APP)
            -- user subscription renewal date (APP)
            -- user number of transactions (APP)
            -- User Date of Last Service Ordered (APP)
            -- work email
            -- secondary email
            ,'mobilephone'  -- mobile phone number
            ,'phone'  -- phone number
            -- secondary phone number
            ,'address'
            ,'address2'
            ,'city'  -- city
            ,'state_province'  -- State / Province
            ,'zip'  -- postal code
            ,'company'  -- company name
            -- brokerage plan member?
            -- listing MLS Number
            ,'brokerage_growth_manager'
            ,'type'
            ,'eligible_for_clients'
            ,'transactly_home_insurance_vendor_status'
            ,'transactly_utility_connection_vendor_status'

            ,'hs_start_date'
            ,'start_date'
            ,'tc_start_date'
        )

)

pivot(
    max(value) for name in(
        'firstname'
        ,'lastname'
        ,'brokerage_name'
        ,'transactly_user_id__app_'
        ,'createdate'
        ,'hs_lead_status'
        ,'email'
        ,'original_sales_repr'
        ,'hubspot_owner_id'
        ,'hs_lifecyclestage_customer_date'
        ,'renewal_date'
        ,'mobilephone'
        ,'phone'
        ,'address'
        ,'address2'
        ,'city'
        ,'state_province'
        ,'zip'
        ,'company'
        ,'brokerage_growth_manager'
        ,'type'
        ,'eligible_for_clients'
        ,'transactly_home_insurance_vendor_status'
        ,'transactly_utility_connection_vendor_status'

        ,'hs_start_date'
        ,'start_date'
        ,'tc_start_date'
    )
)
as p(
    hs_object_id
    ,firstname
    ,lastname
    ,brokerage_name
    ,transactly_user_id__app_
    ,createdate
    ,hs_lead_status
    ,email
    ,original_sales_repr
    ,hubspot_owner_id
    ,hs_lifecyclestage_customer_date
    ,renewal_date
    ,mobilephone
    ,phone
    ,address
    ,address2
    ,city
    ,state_province
    ,zip
    ,company
    ,brokerage_growth_manager
    ,type
    ,eligible_for_clients
    ,transactly_home_insurance_vendor_status
    ,transactly_utility_connection_vendor_status

    ,hs_start_date
    ,start_date
    ,tc_start_date
)
