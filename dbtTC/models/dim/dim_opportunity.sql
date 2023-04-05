-- dim_opportunity

with
    HS_opportunity as(
        select *
        from {{ ref('HS_opportunity')}}
    )

    ,src_sf_contact as(
        select *
        from {{ ref('src_sf_contact')}}
    )

    ,src_sf_opportunity as(
        select *
        from {{ ref('src_sf_opportunity')}}
    )

    ,src_sf_opportunity_line_item as(
        select *
        from {{ ref('src_sf_opportunity_line_item')}}
    )

    ,src_sf_product_2 as(
        select *
        from {{ ref('src_sf_product_2')}}
    )

    ,src_sf_account as(
        select *
        from {{ ref('src_sf_account')}}
    )

    ,sf as(
        select
            -- grain
            p.product_id

            -- opportunity
            ,opp.opportunity_id
            ,opp.opportunity_name
            ,itm.opportunity_line_item_name
            ,opp.stage
            ,opp.close_date as opportunity_close_date
            ,opp.lease_start_date
            ,opp.service_start_date
            ,uo.name as opportunity_owner_name

            -- account and product
            ,p.product_name
            ,p.product_family
            ,a.account_name
            ,a2.account_name as vendor

            -- lead
            --,u.name as owner_name
            --,l.street as lead_street
            --,l.phone as lead_phone
            --,l.mobile_phone as lead_mobile_phone

            -- contact
            ,cont.contact_id
            ,cont.email as contact_email
            ,cont.full_name as contact_full_name
            ,cont.city as contact_city
            ,cont.state as contact_state
            ,cont.phone as contact_phone
            ,cont.mobile_phone as contact_mobile_phone
            ,cont.created_date as contact_created_date
            ,cont.water
            ,u.name as contact_owner_name
            ,cont.attribution as contact_attribution

        from
            src_sf_opportunity opp
            left join src_sf_opportunity_line_item itm on opp.opportunity_id = itm.opportunity_id
            left join src_sf_contact cont on opp.contact_id = cont.contact_id
            --left join src_sf_lead l on cont.converted_lead_c = l.lead_id
            --left join src_sf_user u on l.owner_id = u.user_id
            left join src_sf_product_2 p on itm.product_id = p.product_id
            left join src_sf_account a on opp.account_id = a.account_id
            left join src_sf_account a2 on p.vendor_id = a2.account_id
            left join src_sf_user u on cont.owner_id = u.user_id
            left join src_sf_user uo on opp.owner_id = uo.user_id
    )

    ,final as(
        select
            product_id

            -- opportunity
            ,opportunity_id
            ,opportunity_name
            ,opportunity_line_item_name
            ,null as deal_record_id
            ,stage
            ,opportunity_close_date
            ,lease_start_date
            ,service_start_date
            ,opportunity_owner_name

            -- account and product
            ,product_name
            ,product_family
            ,account_name
            ,vendor

            -- contact
            ,contact_id
            ,contact_email
            ,contact_full_name
            ,contact_city
            ,contact_state
            ,contact_phone
            ,contact_mobile_phone
            ,contact_created_date
            ,water
            ,contact_owner_name
            ,contact_attribution

            ,'SF' as source
        from
            sf

        union select
            null as product_id
            
            ,null as opportunity_id
            ,o.dealname as opportunity_name
            ,null as opportunity_line_item_name
            ,o.deal_record_id
            ,o.dealstage as stage
            ,null as opportunity_close_date
            ,null as lease_start_date
            ,o.service_date_begins as service_start_date
            ,o.owner_name as opportunity_owner_name

            ,o.product_name
            ,o.product_family as product_family
            ,o.account_name
            ,o.vendor

            ,null as contact_id
            ,o.email as contact_email
            ,o.customer_full_name as contact_full_name
            ,o.address as contact_city
            ,null as contact_state
            ,o.phone as contact_phone
            ,o.mobilephone as contact_mobile_phone
            ,null as contact_created_date
            ,null as water
            ,null as contact_owner_name
            ,null as contact_attribution

            ,'HS' as source

        from
            hs_opportunity o

        union select 
            '0', '0', 
            null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null, null, null
    )

select
    working.seq_dim_opportunity.nextval as opportunity_pk
    ,* 
from 
    final

-- select email, opportunity_id, product_name, count(1) from final group by opportunity_id, product_name, email order by count(1) desc
