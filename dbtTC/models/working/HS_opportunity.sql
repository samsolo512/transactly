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

    ,src_hs_pipelines as(
        select *
        from {{ ref('src_hs_pipelines')}}
    )

    ,HS_refine as(
        select 
            objectid, objecttypeid, name, try_to_date(value)
        from 
            src_hs_object_properties 
        where 
            try_to_date(value) is not null
            and lower(name) = 'processed_date'

            -- and objectid = 12871139635
    )

    ,deals as(
        select 
            b.objectid, b.name, b.value
        from 
            HS_refine a
            join src_hs_object_properties b on a.objectid = b.objectid
        -- where
        --     b.name in('dealstage', 'processed_date')
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
            ,customer_full_name
            ,phone
            ,mobilephone
            ,email
            ,account_name
            
        from 
            deals hs
            
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
                    ,'processed_date'
                    ,'revenue'
                    ,'hs_pipeline'
                    ,'vendor_code'
                    ,'customer_full_name'
                    ,'phone'
                    ,'mobilephone'
                    ,'email'
                    ,'company'
            
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
                ,customer_full_name
                ,phone
                ,mobilephone
                ,email
                ,account_name
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
            ,p.customer_full_name
            ,p.phone
            ,p.mobilephone
            ,p.email
            ,p.account_name
            
        from
            HS_pivot p
            left join src_HS_pipeline_stages ps on p.dealstage = ps.stageid
            left join src_HS_pipelines pl on p.pipeline = pl.pipelineid
            left join src_HS_owners o on p.owner_id = o.ownerid
        where
            ps.label = 'Processed'
    )

select * from final
