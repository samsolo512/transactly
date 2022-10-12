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

    ,dim_user as(
        select *
        from {{ ref('dim_user')}}
    )

    ,dim_opportunity as(
        select *
        from {{ ref('dim_opportunity')}}
    )

    ,final as(
        select
            nvl(du.user_pk, 0) as user_pk
            ,nvl(do.opportunity_pk, 0) as opportunity_pk

            ,opp.close_date
            ,opp.is_won_flag
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
            left join src_sf_product_2 p on itm.product_id = p.product_id
            left join dim_user du on l.lead_id = du.lead_id
            left join dim_opportunity do
                on opp.opportunity_id = do.opportunity_id
                and p.product_id = do.product_id
    )

select * from final



-- with
--     src_sf_opportunity as(
--         select *
--         from {{ ref('src_sf_opportunity') }}
--     )
--
--     ,src_sf_opportunity_line_item as(
--         select *
--         from {{ ref('src_sf_opportunity_line_item') }}
--     )
--
--     ,dim_product as(
--         select *
--         from {{ ref('dim_product') }}
--     )
--
--     ,src_sf_contact as(
--         select *
--         from {{ ref('src_sf_contact') }}
--     )
--
--     ,dim_lead as(
--         select *
--         from {{ ref('dim_lead') }}
--     )
--
--     ,final as(
--         select
--             l.lead_pk
--             ,p.product_pk
--
--             ,opp.is_won_flag
--             ,opp.stage
--             ,line.revenue
--             ,case when line.revenue >= 1 then 1 else 0 end as revenue_connection_flag
--             ,case when line.revenue > 0 and line.revenue < 1 then 1 else 0 end as unpaid_connection_flag
--
--         from
--             src_sf_opportunity opp
--             left join src_sf_contact c on c.contact_id = opp.contact_id
--             left join src_sf_opportunity_line_item line on opp.opportunity_id = line.opportunity_id
--             left join dim_product p on line.product_id = p.product_id
--             left join dim_lead l
--                 on c.email = l.email
--                 and c.street = l.street
--                 and opp.opportunity_name = l.opportunity_name
--                 and opp.close_date = l.opportunity_close_date
--                 and opp.created_date = l.opportunity_created_date
--     )
--
-- select * from final