with
    src_sf_opportunity as(
        select *
        from {{ ref('src_sf_opportunity') }}
    )

    ,src_sf_opportunity_line_item as(
        select *
        from {{ ref('src_sf_opportunity_line_item') }}
    )

    ,dim_opportunity as(
        select *
        from {{ ref('dim_opportunity') }}
    )

    ,dim_product as(
        select *
        from {{ ref('dim_product') }}
    )

    ,dim_date as(
        select *
        from {{ ref('dim_date') }}
    )

    ,final as(
        select
            o.opportunity_pk
            ,p.product_pk
            ,d.date_pk as close_date_pk
            ,case
                when opp.is_won_flag = 'TRUE' then 1
                when opp.is_won_flag = 'FALSE' then 0
                else 0
                end as is_won_flag
            ,opp.stage
            ,line.revenue
        from
            src_sf_opportunity opp
            left join src_sf_opportunity_line_item line on opp.opportunity_id = line.opportunity_id
            left join dim_opportunity o on opp.opportunity_id = o.opportunity_id
            left join dim_product p on line.product_id = p.product_id
            left join dim_date d on opp.close_date = d.date_id
    )

select * from final