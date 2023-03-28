-- HS_opportunity

{{
    config(
        materialized = 'table'
    )
}}

with
    src_hs_pipeline_stages as(
        select *
        from {{ ref('src_hs_pipeline_stages')}}
    )

    ,src_hs_object_properties as(
        select *
        from {{ ref('src_hs_object_properties')}}
    )

    ,src_hs_owners as(
        select *
        from {{ ref('src_hs_owners')}}
    )

    ,HS_refine as(
        select distinct
            objectid
        from
            src_hs_object_properties
        where
            name = 'deal_record_id'
            and value is not null
    )

    ,starting as(
        select 
            a.objectid
            ,a.name
            ,a.value 
        from 
            src_hs_object_properties a
            join HS_refine b on a.objectid = b.objectid
    )

    ,HS_pivot as(
        select 
            objectid
            ,opportunity_name
            ,opportunity_id
            ,stage
            ,hs_createdate
            ,vendor
            ,product_name
            ,address
            ,hs_record_id
            ,customer_full_name
        from 
            starting hs
            
            pivot(
                max(value) for name in(
                    'dealname'
                    ,'opportunity_id'
                    ,'hs_pipeline_stage'
                    ,'hs_createdate'
                    ,'vendor_name'
                    ,'vendor_code'
                    ,'customer_address'
                    ,'deal_record_id'
                    ,'customer_full_name'
                )
            ) as p (
                objectid
                ,opportunity_name
                ,opportunity_id
                ,stage
                ,hs_createdate
                ,vendor
                ,product_name
                ,address
                ,hs_record_id
                ,customer_full_name
            )
    )

    ,final as(
        select
            -- p.objectid
            p.opportunity_name
            ,p.opportunity_id
            ,ps.label as stage
            ,p.vendor
            ,try_to_date(p.hs_createdate) as hs_createdate
            ,p.product_name
            ,p.address
            {# ,o.email as agent_email #}
            ,p.hs_record_id
            ,p.customer_full_name
        from
            HS_pivot p
            left join src_HS_pipeline_stages ps on p.stage = ps.stageid
            {# left join src_HS_owners o on p.opportunity_owner_id = to_varchar(o.ownerid) #}
    )

select * from final
