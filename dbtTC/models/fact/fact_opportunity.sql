-- fact_opportunity

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

    ,dim_lead as(
        select *
        from {{ ref('dim_lead')}}
    )

    ,dim_opportunity as(
        select *
        from {{ ref('dim_opportunity')}}
    )

    ,final as(
        select
            nvl(ld.lead_pk, 0) as lead_pk
            ,nvl(do.opportunity_pk, 0) as opportunity_pk
            ,nvl(ag.agent_pk, 0) as agent_pk

            ,opp.close_date
            ,opp.created_date
            ,case when itm.revenue >= 1 then 1 else 0 end as revenue_connection_flag
            ,case when itm.revenue > 0 and itm.revenue < 1 then 1 else 0 end as unpaid_connection_flag
            ,itm.revenue

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
    )

select * from final

