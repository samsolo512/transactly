with
    src_sf_opportunity as(
        select *
        from {{ ref('src_sf_opportunity') }}
    )

    ,src_sf_opportunity_line_item as(
        select *
        from {{ ref('src_sf_opportunity_line_item') }}
    )

    ,dim_product as(
        select *
        from {{ ref('dim_product') }}
    )

    ,src_sf_contact as(
        select *
        from {{ ref('src_sf_contact') }}
    )

    ,dim_lead as(
        select *
        from {{ ref('dim_lead') }}
    )

    ,final as(
        select
            l.lead_pk
            ,p.product_pk

            ,opp.is_won_flag
            ,opp.stage
            ,line.revenue
            ,case when line.revenue >= 1 then 1 else 0 end as revenue_connection_flag
            ,case when line.revenue > 0 and line.revenue < 1 then 1 else 0 end as unpaid_connection_flag

        from
            src_sf_opportunity opp
            left join src_sf_contact c on c.contact_id = opp.contact_id
            left join src_sf_opportunity_line_item line on opp.opportunity_id = line.opportunity_id
            left join dim_product p on line.product_id = p.product_id
            left join dim_lead l
                on c.email = l.email
                and c.street = l.street
                and opp.opportunity_name = l.opportunity_name
                and opp.close_date = l.opportunity_close_date
                and opp.created_date = l.opportunity_created_date
    )

select * from final