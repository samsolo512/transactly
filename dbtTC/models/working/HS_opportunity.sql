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

    ,src_hs_pipelines as(
        select *
        from {{ ref('src_hs_pipelines')}}
    )

    ,HS_refine as(
        select distinct
            value
            ,objectid
        from
            src_hs_object_properties
        where
            name = 'deal_record_id'
    )

    ,starting as(
        select
            b.objectid
            ,a.name
            ,a.value
        from 
            src_hs_object_properties a
            join HS_refine b on a.objectid = b.value

        union select 
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
            ,dealname
            ,deal_record_id
            ,dealstage
            ,nullif(trim(owner_id), '') as owner_id
            ,product_name
            ,product_family
            ,vendor
            ,address
            ,contact_attribution
            ,service_date_begins
            ,create_date
            ,closed_date
            ,revenue
            ,pipeline
            ,vendor_code
            
        from 
            starting hs
            
            pivot(
                max(value) for name in(
                    'dealname'
                    ,'deal_record_id'
                    ,'dealstage'
                    ,'hubspot_owner_id'
                    ,'product_code'
                    ,'product_families__c'
                    ,'vendor_name'
                    ,'customer_address'
                    ,'attribution__c'
                    ,'service_date_begins__c'
                    ,'hs_createdate'
                    ,'closedate'
                    ,'revenue'
                    ,'hs_pipeline'
                    ,'vendor_code'
                )
            ) as p (
                objectid
                ,dealname
                ,deal_record_id
                ,dealstage
                ,owner_id
                ,product_name
                ,product_family
                ,vendor
                ,address
                ,contact_attribution
                ,service_date_begins
                ,create_date
                ,closed_date
                ,revenue
                ,pipeline
                ,vendor_code
            )
    )

    ,final as(
        select
            -- p.objectid
            p.dealname
            ,p.deal_record_id
            ,ps.label as dealstage
            ,concat(o.firstname, ' ', o.lastname) as owner_name
            ,p.product_name
            ,p.product_family
            ,p.vendor
            ,p.address
            ,p.contact_attribution
            ,try_to_date(p.service_date_begins) as service_date_begins
            ,try_to_date(p.create_date) as create_date
            ,try_to_date(p.closed_date) as closed_date
            ,try_to_number(p.revenue) as revenue
            ,pl.label as pipeline
            ,p.vendor_code
            
        from
            HS_pivot p
            left join src_HS_pipeline_stages ps on p.dealstage = ps.stageid  -- select top 10 * from src_HS_pipeline
            left join src_HS_pipelines pl on p.pipeline = pl.pipelineid
            left join src_HS_owners o on p.owner_id = o.ownerid
    )

select * from final
