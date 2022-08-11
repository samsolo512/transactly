with
    src_sf_opportunity as(
        select *
        from {{ ref('src_sf_opportunity')}}
    )

    ,src_sf_opportunity_line_item as(
        select *
        from {{ ref('src_sf_opportunity_line_item')}}
    )

    ,src_sf_account as(
        select *
        from {{ ref('src_sf_account')}}
    )

    ,src_sf_contact as(
        select *
        from {{ ref('src_sf_contact')}}
    )

    ,final as(
        select
            working.seq_dim_opportunity.nextval as opportunity_pk
            ,opp.opportunity_id
            ,opp.opportunity_name
            ,acc.account_name
            ,c.street
            ,c.state
        from
            src_sf_opportunity opp
            left join src_sf_opportunity_line_item line on opp.opportunity_id = line.opportunity_id
            left join src_sf_account acc on opp.account_id = acc.account_id
            left join src_sf_contact c on opp.contact_id = c.contact_id
    )

select * from final