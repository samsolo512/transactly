-- dim_opportunity

with
    src_sf_lead as(
        select *
        from {{ ref('src_sf_lead')}}
    )

    ,src_sf_contact as(
        select *
        from {{ ref('src_sf_contact')}}
    )

    ,src_sf_opportunity as(
        select *
        from {{ ref('src_sf_opportunity')}}
    )

    ,src_sf_user as(
        select *
        from {{ ref('src_sf_user')}}
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

    ,final as(
        select
            working.seq_dim_opportunity.nextval as opportunity_pk

            -- grain
            ,opp.opportunity_id
            ,p.product_id

            ,u.name as owner_name
            ,opp.stage
            ,itm.opportunity_line_item_name
            ,opp.opportunity_name
            ,p.product_name
            ,p.product_family
            ,a.account_name
            ,cont.contact_id
            ,cont.email
            ,cont.full_name
            ,cont.city
            ,cont.state
            ,cont.created_date
            ,cont.water
            ,cont.full_name as contact_full_name
            ,opp.close_date as opportunity_close_date
            ,opp.lease_start_date
            ,l.phone

        from
            src_sf_opportunity opp
            left join src_sf_opportunity_line_item itm on opp.opportunity_id = itm.opportunity_id
            left join src_sf_contact cont on opp.contact_id = cont.contact_id
            left join src_sf_lead l on cont.converted_lead_c = l.lead_id
            left join src_sf_user u on l.owner_id = u.user_id
            left join src_sf_product_2 p on itm.product_id = p.product_id
            left join src_sf_account a on opp.account_id = a.account_id

        union select 
            0, '0', '0', 
            null, null, null, null, null, null, null, null, null, 
            null, null, null, null, null, null, null, null, null
    )

select * from final

-- select email, opportunity_id, product_name, count(1) from final group by opportunity_id, product_name, email order by count(1) desc
