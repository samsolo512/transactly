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
            objectid
            ,value
        from
            src_hs_object_properties
        where
            name = 'deal_record_id'
    )

    ,deal_value as(
        select
            b.objectid
            ,a.name
            ,a.value
        from 
            src_hs_object_properties a
            join HS_refine b on a.objectid = b.value
    )

    ,deal_object as(
        select 
            a.objectid
            ,a.name
            ,a.value 
        from 
            src_hs_object_properties a
            join HS_refine b on a.objectid = b.objectid
    )

    ,deal_value_contact as(
        select 
            a.objectid
            ,a.objecttypeid
            ,a.name
            ,a.value
            ,b.objectid as aliasobjectid
        from 
            src_hs_object_properties a
            join deal_value b
                on a.value = b.value
                and a.name = 'hs_analytics_source_data_2'
                and b.name = 'hs_analytics_source_data_2'
                and objecttypeid = '0-1'
    )

    ,deal_contact as(
        select 
            b.aliasobjectid as objectid
            ,a.name
            ,a.value
        from 
            src_hs_object_properties a
            join deal_value_contact b 
                on a.objectid = b.objectid
                and a.objecttypeid = b.objecttypeid
    )

    ,combine as(
        select
            a.objectid
            ,a.name
            ,a.value
        from deal_value a

        union select
            b.objectid
            ,b.name
            ,b.value
        from deal_object b

        union select
            c.objectid
            ,c.name
            ,c.value
        from deal_contact c
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
            combine hs
            
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
    )

select * from final
