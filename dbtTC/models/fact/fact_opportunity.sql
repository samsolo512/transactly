-- fact_opportunity
-- 1 row/opportunity line item

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

    ,src_sf_opportunity_line_item as(
        select *
        from {{ ref('src_sf_opportunity_line_item')}}
    )

    ,src_sf_product_2 as(
        select *
        from {{ ref('src_sf_product_2')}}
    )
    
    ,src_sf_vendor_payout_c as(
        select *
        from {{ ref('src_sf_vendor_payout_c')}}
    )

    ,HS_opportunity as(
        select *
        from {{ ref('HS_opportunity')}}
    )

    ,dim_lead as(
        select *
        from {{ ref('dim_lead')}}
    )

    ,dim_opportunity as(
        select *
        from {{ ref('dim_opportunity')}}
    )

    ,dim_agent as(
        select *
        from {{ ref('dim_agent')}}
    )
    
    ,SF as(
        select
            nvl(ld.lead_pk, (select lead_pk from dim_lead where lead_id = '0')) as lead_pk
            ,nvl(do.opportunity_pk, 0) as opportunity_pk
            ,nvl(ag.agent_pk, 0) as agent_pk

            ,opp.created_date
            ,opp.close_date
            ,datediff(day, opp.created_date, opp.close_date) as days_to_close
            ,opp.last_stage_change_date
            ,case when itm.revenue >= 1 then 1 else 0 end as revenue_connection_flag
            --,case when itm.revenue > 0 and itm.revenue < 1 then 1 else 0 end as unpaid_connection_flag
            ,case 
                when v.opportunity_id is not null and itm.revenue > 0 then 0
                else 1
                end as unpaid_connection_flag
            ,itm.revenue
            ,datediff(day, opp.created_date, getdate()) as days_since_created
            ,opp.opportunity_id

        from
            src_sf_opportunity opp
            left join(
                select
                    opportunity_id
                    ,product_id
                    ,sum(revenue) as revenue
                from
                    src_sf_opportunity_line_item itm
                group by opportunity_id, product_id
            ) itm on opp.opportunity_id = itm.opportunity_id
            left join src_sf_contact cont on opp.contact_id = cont.contact_id
            left join src_sf_lead l on cont.converted_lead_c = l.lead_id
            left join dim_lead ld on l.lead_id = ld.lead_id
            left join src_sf_product_2 p on itm.product_id = p.product_id
            left join dim_opportunity do
                on opp.opportunity_id = do.opportunity_id
                and p.product_id = do.product_id
            left join dim_agent ag on l.agent_email = ag.agent_email
            left join src_sf_vendor_payout_c v on v.opportunity_id = opp.opportunity_id
    )

    ,final as(
        -- salesforce
        select
            -- grain
            lead_pk
            ,opportunity_pk
            ,agent_pk

            ,created_date
            ,close_date
            ,days_to_close
            ,last_stage_change_date
            ,revenue_connection_flag
            ,unpaid_connection_flag
            ,days_since_created
            ,'SF' as source
            ,sum(revenue) as revenue
        from
            SF
        group by
            1,2,3,4,5,6,7,8,9,10,11

        -- hubspot
        union all select
            (select lead_pk from dim_lead where lead_id = '0') as lead_pk
            ,ifnull(o.opportunity_pk, (select opportunity_pk from dim_opportunity where opportunity_id = '0')) as opportunity_pk
            ,(select agent_pk from dim_agent where agent_email is null) as agent_pk
            
            ,f.create_date as created_date
            ,f.closed_date as close_date
            ,datediff(day, f.create_date, f.closed_date) as days_to_close
            ,null as last_stage_change_date
            ,case when f.revenue >= 1 then 1 else 0 end as revenue_connection_flag
            ,case 
                when f.revenue > 0 then 0
                else 1
                end as unpaid_connection_flag
            ,datediff(day, f.create_date, getdate()) as days_since_created
            ,'HS' as source
            ,f.revenue
            
            -- ,o.opportunity_name
            -- ,o.opportunity_id
            -- ,o.label as stage
            -- ,o.product_family
            -- ,o.address
        from
            HS_opportunity f
            left join dim_opportunity o on f.dealname = o.opportunity_name
    )

select * from final
